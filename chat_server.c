/*
 * chat_server.c — A multi-threaded TCP chat server.
 *
 * Design:
 *   - Listens on a configurable TCP port (argv[1], default PORT below).
 *   - Accepts many clients; each accepted connection is handled by its own
 *     *detached* pthread so we never need to join them.
 *   - All connected clients live in a global array guarded by a single mutex.
 *   - Supported client commands (one per text line):
 *       JOIN <room_name>          -> create the room if needed, add client as a
 *                                    member, reply "OK joined <room_name>"
 *       PRIORITY <room> <n>       -> (admin only) set a room's scheduling weight
 *                                    at runtime; higher = served more often
 *       <any other text>          -> enqueue the message onto the client's room
 *
 * Rooms are first-class objects (room_t): each has an ID, a name, a member
 * list, and its own FIFO message queue. The global list of rooms is protected
 * by `rooms_mutex`; each room's members+queue are protected by that room's own
 * `lock`.
 *
 * Delivery is done by a small pool of worker threads (default 3), deliberately
 * fewer than the number of rooms so contention is visible. A shared "ready
 * room" set (rooms with pending messages) is protected by `ready_mutex`; the
 * scheduler next_ready_room() hands rooms out round-robin, never giving the
 * same room to two workers at once. A worker locks the room, drains and
 * delivers its queue to every member as "[<room>] <sender>: <text>", unlocks,
 * and releases the room back to the scheduler.
 *
 * Workers never busy-wait. When no room is serveable they block on the
 * condition variable `ready_cond` (via a predicate loop, so spurious wakeups
 * are harmless), consuming zero CPU until a producer enqueues a message and
 * signals one worker awake.
 *
 * Persistence: after a worker delivers a message, it also writes the record
 * "<room>|<sender>|<text>" to a named pipe (FIFO). A separate `logger` process
 * reads the FIFO and appends timestamped lines to chat_history.log, so all
 * disk I/O happens outside the server. The server opens the FIFO write end
 * non-blocking and ignores SIGPIPE: if the logger is down or slow the server
 * drops the record rather than blocking, and transparently reopens the FIFO
 * when the logger comes back.
 *
 * Scheduling is swappable. Two strategies sit behind one selector signature so
 * the worker loop never changes: --scheduler round_robin (equal turns) or
 * --scheduler priority (smooth weighted round-robin, where a room of priority N
 * is served N times as often as a priority-1 room). Priorities can be changed
 * live with the admin PRIORITY command, so a demo can show the scheduler react.
 * The first client to connect is designated the admin.
 *
 * Build:  make                         (builds ./chat_server and ./logger)
 * Run:    ./logger &                   (start the logger first, or any time)
 *         ./chat_server <port> <num_workers> [--scheduler round_robin|priority]
 *         (e.g. ./chat_server 5000 3 --scheduler priority)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <stdint.h>
#include <fcntl.h>
#include <signal.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>

#define DEFAULT_PORT     5000
#define DEFAULT_WORKERS  3
#define MAX_CLIENTS      128
#define MAX_ROOMS        64
#define MAX_MEMBERS      128
#define MAX_WORKERS      64
#define NAME_LEN         32
#define BUF_LEN          1024
#define SERVE_QUANTUM    1               /* messages delivered per scheduling turn */
#define FIFO_PATH        "chat_fifo"     /* named pipe to the logger process */

/* ------------------------------------------------------------------ */
/* Client bookkeeping                                                 */
/* ------------------------------------------------------------------ */

/*
 * One entry per connected client. `room_id` is -1 when the client has not
 * joined any room yet.
 */
typedef struct {
    int  fd;              /* connected socket file descriptor            */
    int  id;              /* unique, monotonically increasing client ID  */
    char name[NAME_LEN];  /* display name (default "clientN")            */
    int  room_id;         /* current room, or -1 if none                 */
    int  is_admin;        /* 1 if this client may run admin commands     */
} client_t;

/*
 * Global list of clients. A NULL slot means "empty". Everything that touches
 * `clients` (including reading it) must hold `clients_mutex`.
 */
static client_t *clients[MAX_CLIENTS];
static pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Source of unique client IDs; only touched under clients_mutex. */
static int next_client_id = 1;

/* The first client to connect is designated the admin (the only client allowed
 * to run PRIORITY). Guarded by clients_mutex. Connect your control terminal
 * first during a demo. */
static int admin_assigned = 0;

/* ------------------------------------------------------------------ */
/* Rooms and per-room message queues                                  */
/* ------------------------------------------------------------------ */

/*
 * One queued chat message. Messages form a singly linked FIFO list inside a
 * room (q_head -> ... -> q_tail). Each carries the sender's display name and
 * the message text.
 */
typedef struct message {
    char sender[NAME_LEN];
    char text[BUF_LEN];
    struct message *next;
} message_t;

/*
 * A chat room. `members` holds the IDs of clients currently in the room;
 * `-1` marks an empty slot. The message queue and member list are both
 * protected by `lock` (one mutex per room), so different rooms never contend.
 */
typedef struct {
    int  id;                       /* room ID (also its index in rooms[])  */
    char name[NAME_LEN];           /* room name                            */
    int  members[MAX_MEMBERS];     /* member client IDs; -1 = empty slot   */
    int  member_count;             /* number of occupied member slots      */
    message_t *q_head;             /* head of the FIFO message queue       */
    message_t *q_tail;             /* tail of the FIFO message queue       */
    size_t     q_len;              /* number of messages currently queued  */
    pthread_mutex_t lock;          /* protects members + queue of THIS room */

    /* Scheduler bookkeeping — protected by ready_mutex, NOT by `lock`. */
    int in_ready;                  /* 1 if room is in the ready set        */
    int servicing;                 /* 1 if a worker is currently serving it */
    int  priority;                 /* scheduling weight, >= 1 (default 1)  */
    long cw;                       /* current weight for the weighted RR    */
} room_t;

/*
 * Global list of rooms. A NULL slot means "empty". `rooms_mutex` guards the
 * array itself (allocation, lookup, count) — NOT the internals of any room,
 * which each have their own `lock`.
 */
static room_t *rooms[MAX_ROOMS];
static int room_count = 0;
static pthread_mutex_t rooms_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * Find the room named `name`, creating it if it does not exist. Returns the
 * room pointer, or NULL if the room table is full. Locks `rooms_mutex`
 * internally. The returned pointer stays valid for the lifetime of the server
 * (rooms are never destroyed here), so it is safe to use after unlocking.
 */
static room_t *find_or_create_room(const char *name)
{
    room_t *r = NULL;
    pthread_mutex_lock(&rooms_mutex);

    /* Existing room? */
    for (int i = 0; i < room_count; i++) {
        if (rooms[i] != NULL && strncmp(rooms[i]->name, name, NAME_LEN) == 0) {
            r = rooms[i];
            break;
        }
    }

    /* Otherwise create it, if there is space. */
    if (r == NULL && room_count < MAX_ROOMS) {
        r = calloc(1, sizeof(room_t));
        if (r != NULL) {
            r->id = room_count;
            strncpy(r->name, name, NAME_LEN - 1);
            r->name[NAME_LEN - 1] = '\0';
            for (int i = 0; i < MAX_MEMBERS; i++)
                r->members[i] = -1;
            r->member_count = 0;
            r->q_head = r->q_tail = NULL;
            r->q_len = 0;
            r->priority = 1;        /* default weight; PRIORITY cmd changes it */
            r->cw = 0;              /* weighted-RR running credit             */
            pthread_mutex_init(&r->lock, NULL);

            rooms[room_count] = r;
            room_count++;
        }
    }

    pthread_mutex_unlock(&rooms_mutex);
    return r;
}

/*
 * Look up a room by its ID. Returns NULL if not found. Locks `rooms_mutex`
 * internally.
 */
static room_t *find_room_by_id(int id)
{
    room_t *r = NULL;
    pthread_mutex_lock(&rooms_mutex);
    if (id >= 0 && id < room_count)
        r = rooms[id];
    pthread_mutex_unlock(&rooms_mutex);
    return r;
}

/*
 * Look up an existing room by name (does NOT create it). Returns NULL if there
 * is no such room. Locks `rooms_mutex` internally.
 */
static room_t *find_room_by_name(const char *name)
{
    room_t *r = NULL;
    pthread_mutex_lock(&rooms_mutex);
    for (int i = 0; i < room_count; i++) {
        if (rooms[i] != NULL && strncmp(rooms[i]->name, name, NAME_LEN) == 0) {
            r = rooms[i];
            break;
        }
    }
    pthread_mutex_unlock(&rooms_mutex);
    return r;
}

/*
 * Add `client_id` to a room's member list (idempotent). Returns 0 on success,
 * -1 if the member list is full. Locks the room's own `lock`.
 */
static int room_add_member(room_t *r, int client_id)
{
    int rc = -1;
    pthread_mutex_lock(&r->lock);

    /* Already a member? Treat as success. */
    for (int i = 0; i < MAX_MEMBERS; i++) {
        if (r->members[i] == client_id) {
            rc = 0;
            goto out;
        }
    }
    /* Insert into first free slot. */
    for (int i = 0; i < MAX_MEMBERS; i++) {
        if (r->members[i] == -1) {
            r->members[i] = client_id;
            r->member_count++;
            rc = 0;
            goto out;
        }
    }
out:
    pthread_mutex_unlock(&r->lock);
    return rc;
}

/*
 * Append a message to a room's FIFO queue. On success returns the resulting
 * queue length (>= 1); returns -1 on allocation failure. Locks the room's own
 * `lock`, and reports the length while still holding it so callers get a
 * consistent value.
 */
static long room_enqueue(room_t *r, const char *sender, const char *text)
{
    message_t *m = calloc(1, sizeof(message_t));
    if (m == NULL)
        return -1;

    strncpy(m->sender, sender, NAME_LEN - 1);
    m->sender[NAME_LEN - 1] = '\0';
    strncpy(m->text, text, BUF_LEN - 1);
    m->text[BUF_LEN - 1] = '\0';
    m->next = NULL;

    pthread_mutex_lock(&r->lock);
    if (r->q_tail == NULL) {
        r->q_head = r->q_tail = m;      /* first message */
    } else {
        r->q_tail->next = m;            /* append at tail */
        r->q_tail = m;
    }
    r->q_len++;
    long len = (long)r->q_len;
    pthread_mutex_unlock(&r->lock);
    return len;
}

/* ------------------------------------------------------------------ */
/* Ready-room scheduler (shared by the worker pool)                   */
/* ------------------------------------------------------------------ */

/*
 * The "ready set" is the list of rooms that currently have pending messages
 * and therefore need a worker's attention. It is a plain array of room
 * pointers (pointers are stable for the server's lifetime), plus a round-robin
 * cursor. Everything here — including the per-room in_ready/servicing flags —
 * is protected by `ready_mutex`. `ready_cond` lets idle workers sleep until a
 * room becomes available.
 */
static room_t *ready_rooms[MAX_ROOMS];
static int ready_count  = 0;
static int ready_cursor = 0;                                   /* round-robin position */
static pthread_mutex_t ready_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  ready_cond  = PTHREAD_COND_INITIALIZER;

/*
 * Announce that `r` has pending messages. Adds it to the ready set (unless
 * already present) and wakes one waiting worker. Called by producers right
 * after enqueuing a message.
 */
static void mark_room_ready(room_t *r)
{
    pthread_mutex_lock(&ready_mutex);
    if (!r->in_ready) {
        r->in_ready = 1;
        ready_rooms[ready_count++] = r;     /* at most one slot per room */
        pthread_cond_signal(&ready_cond);
    }
    pthread_mutex_unlock(&ready_mutex);
}

/*
 * A "selector" chooses which serveable room a worker should take next. Both
 * strategies share one signature and one contract:
 *   - Called with `ready_mutex` held.
 *   - Return the index into ready_rooms[] of the chosen room, or -1 if no room
 *     is serveable right now (ready AND not already being served).
 *   - May update strategy-private state (the round-robin cursor, the weighted
 *     credits) but must NOT set `servicing` — the shared wrapper does that.
 * The active strategy is chosen once at startup via `select_serveable_locked`,
 * so the worker loop and next_ready_room() stay identical for both.
 */
typedef int (*selector_fn)(void);

/*
 * Round-robin selector: hand out serveable rooms in rotation from
 * `ready_cursor`, giving every ready room equal turns.
 */
static int select_rr_locked(void)
{
    for (int scanned = 0; scanned < ready_count; scanned++) {
        int idx = (ready_cursor + scanned) % ready_count;
        if (!ready_rooms[idx]->servicing) {
            ready_cursor = (idx + 1) % ready_count;   /* resume after this one */
            return idx;
        }
    }
    return -1;
}

/*
 * Priority selector: smooth weighted round-robin (the algorithm nginx uses).
 * Each serveable room's running credit `cw` is bumped by its weight (priority);
 * we pick the room with the greatest credit and then subtract the total weight
 * from it. Over a full cycle a room of priority N is chosen N times as often as
 * a priority-1 room, and the picks are interleaved smoothly rather than bursty.
 * Rooms currently being served are skipped and do not accrue credit that turn.
 */
static int select_priority_locked(void)
{
    int best = -1;
    long total = 0;

    for (int i = 0; i < ready_count; i++) {
        room_t *r = ready_rooms[i];
        if (r->servicing)
            continue;
        int w = (r->priority > 0) ? r->priority : 1;
        total += w;
        r->cw += w;
        if (best < 0 || r->cw > ready_rooms[best]->cw)
            best = i;
    }

    if (best >= 0)
        ready_rooms[best]->cw -= total;

    return best;
}

/* The active selection strategy, set from --scheduler in main(). */
static selector_fn select_serveable_locked = select_rr_locked;

/*
 * Scheduler: return the next room needing service according to the active
 * strategy, marking it `servicing` so no other worker grabs it. Never returns
 * NULL. Its signature is fixed regardless of strategy, so the worker loop never
 * changes.
 *
 * No busy-waiting: when nothing is serveable the worker blocks in
 * pthread_cond_wait(), which atomically releases `ready_mutex` and parks the
 * thread. A parked thread is off the run queue and consumes no CPU until it is
 * signaled. The `while` condition is a genuine predicate loop — it is
 * re-evaluated after every wakeup, so a spurious wakeup (or a wakeup that
 * another worker "won" first) simply finds no serveable room and goes back to
 * sleep, rather than proceeding on a false assumption.
 */
static room_t *next_ready_room(int worker_id)
{
    pthread_mutex_lock(&ready_mutex);

    int idx;
    while ((idx = select_serveable_locked()) < 0) {
        /* Predicate is false: no serveable room. Log once, then block. If the
         * server is quiet you will see this line and then nothing more from
         * this worker — proof it is parked, not spinning. */
        printf("Worker %d: no ready rooms — blocking on condition variable\n",
               worker_id);
        fflush(stdout);

        pthread_cond_wait(&ready_cond, &ready_mutex);   /* sleeps, 0% CPU */

        /* Woken (possibly spuriously): loop re-checks the predicate. */
        printf("Worker %d: woken — re-checking (ready_count=%d)\n",
               worker_id, ready_count);
        fflush(stdout);
    }

    room_t *r = ready_rooms[idx];
    r->servicing = 1;

    pthread_mutex_unlock(&ready_mutex);
    return r;
}

/*
 * Change a room's scheduling priority (weight) at runtime. Clamped to >= 1.
 * Runs under `ready_mutex` because the selectors read `priority`/`cw` there.
 * Resets the weighted-RR credit so the new weight takes effect cleanly.
 */
static void set_room_priority(room_t *r, int priority)
{
    if (priority < 1)
        priority = 1;
    pthread_mutex_lock(&ready_mutex);
    r->priority = priority;
    r->cw = 0;
    pthread_mutex_unlock(&ready_mutex);
}

/*
 * A worker calls this after servicing a room. Clears `servicing`. If new
 * messages arrived while it was being served, the room stays in the ready set
 * (and we wake a worker); otherwise it is removed from the set.
 */
static void release_room(room_t *r)
{
    pthread_mutex_lock(&ready_mutex);
    r->servicing = 0;

    /* Did more messages arrive during delivery? */
    pthread_mutex_lock(&r->lock);
    int still_pending = (r->q_head != NULL);
    pthread_mutex_unlock(&r->lock);

    if (still_pending) {
        pthread_cond_signal(&ready_cond);        /* keep it ready, wake someone */
    } else {
        r->in_ready = 0;                         /* remove from the ready set */
        for (int i = 0; i < ready_count; i++) {
            if (ready_rooms[i] == r) {
                ready_rooms[i] = ready_rooms[ready_count - 1];   /* swap-remove */
                ready_count--;
                if (ready_count > 0)
                    ready_cursor %= ready_count;                 /* keep cursor valid */
                else
                    ready_cursor = 0;
                break;
            }
        }
    }
    pthread_mutex_unlock(&ready_mutex);
}

/* ------------------------------------------------------------------ */
/* Client list helpers (all assume the caller-appropriate locking)    */
/* ------------------------------------------------------------------ */

/* Insert `c` into the first free slot. Returns 0 on success, -1 if full.
 * Caller must hold clients_mutex. */
static int add_client(client_t *c)
{
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i] == NULL) {
            clients[i] = c;
            return 0;
        }
    }
    return -1;
}

/* Remove the client with the given id from the list.
 * Caller must hold clients_mutex. */
static void remove_client(int id)
{
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i] != NULL && clients[i]->id == id) {
            clients[i] = NULL;
            return;
        }
    }
}

/* Return the socket fd for the client with the given id, or -1 if that client
 * is no longer connected. Locks clients_mutex internally. */
static int fd_for_client(int id)
{
    int fd = -1;
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i] != NULL && clients[i]->id == id) {
            fd = clients[i]->fd;
            break;
        }
    }
    pthread_mutex_unlock(&clients_mutex);
    return fd;
}

/* Send a NUL-terminated C string on a socket, retrying on short writes. */
static void send_line(int fd, const char *s)
{
    size_t len = strlen(s);
    size_t off = 0;
    while (off < len) {
        ssize_t n = write(fd, s + off, len - off);
        if (n <= 0) {
            if (n < 0 && errno == EINTR)
                continue;      /* interrupted, retry */
            break;             /* peer gone / error: give up */
        }
        off += (size_t)n;
    }
}

/* ------------------------------------------------------------------ */
/* Persistence: non-blocking writes to the logger via the FIFO        */
/* ------------------------------------------------------------------ */

/*
 * Write end of the FIFO. -1 means "not currently open" (logger absent). Guarded
 * by log_mutex because any worker thread may write concurrently.
 */
static int log_fd = -1;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * Try to (re)open the FIFO for writing without blocking. Opening the write end
 * with O_NONBLOCK returns -1/ENXIO when no reader (logger) is attached yet,
 * instead of blocking forever — exactly what we want. Caller holds log_mutex.
 */
static int log_try_open(void)
{
    return open(FIFO_PATH, O_WRONLY | O_NONBLOCK);   /* -1 if no logger yet */
}

/*
 * Persist one delivered message by writing "<room>|<sender>|<text>\n" to the
 * FIFO. This NEVER blocks the server:
 *   - If the logger is not attached, the (re)open fails and we drop the record.
 *   - If the logger is attached but its pipe buffer is full (slow disk), the
 *     non-blocking write returns EAGAIN and we drop the record.
 *   - If the logger disconnected, the write returns EPIPE (SIGPIPE is ignored
 *     process-wide); we close our end so the next call transparently reopens
 *     once the logger reconnects.
 * Dropping under back-pressure is the deliberate trade-off: delivery to live
 * clients must not stall on disk I/O.
 */
static void log_message(const char *room, const char *sender, const char *text)
{
    char line[NAME_LEN + NAME_LEN + BUF_LEN + 8];
    int n = snprintf(line, sizeof(line), "%s|%s|%s\n", room, sender, text);
    if (n < 0)
        return;
    size_t len = (n >= (int)sizeof(line)) ? sizeof(line) - 1 : (size_t)n;

    pthread_mutex_lock(&log_mutex);

    if (log_fd < 0)
        log_fd = log_try_open();          /* logger may have (re)started */

    if (log_fd >= 0) {
        ssize_t w = write(log_fd, line, len);
        if (w < 0) {
            if (errno == EPIPE) {
                /* Logger went away: drop our end, reopen on the next message. */
                close(log_fd);
                log_fd = -1;
            }
            /* EAGAIN/EWOULDBLOCK (pipe full) or any other error: drop record. */
        }
    }

    pthread_mutex_unlock(&log_mutex);
}

/* ------------------------------------------------------------------ */
/* Worker pool: deliver queued messages                               */
/* ------------------------------------------------------------------ */

/*
 * Worker thread. Loops forever:
 *   1. next_ready_room() — block until a room needs service.
 *   2. Lock that room, pop up to SERVE_QUANTUM messages from the head, and
 *      deliver each to every current member as "[<room>] <sender>: <text>".
 *   3. Unlock the room and release it back to the scheduler.
 *
 * Serving a bounded quantum (rather than draining the whole queue) matters for
 * two reasons: (a) contention stays visible — a room "in service" is off-limits
 * to other workers, so extra rooms wait their turn; and (b) the priority
 * scheduler can actually give higher-priority rooms proportionally more turns.
 * A backlogged room returns to the scheduler after each quantum instead of
 * monopolizing the worker, so with SERVE_QUANTUM == 1 a priority-N room is
 * served (and thus delivered) N times as often as a priority-1 room.
 *
 * Delivery is done while holding the room lock (per the design).
 */
static void *worker_thread(void *arg)
{
    int worker_id = (int)(intptr_t)arg;

    for (;;) {
        room_t *r = next_ready_room(worker_id);   /* blocks until one is ready */

        printf("Worker %d serving room '%s' (id=%d)\n",
               worker_id, r->name, r->id);
        fflush(stdout);

        pthread_mutex_lock(&r->lock);

        /* Pop and deliver up to SERVE_QUANTUM messages from the head. Anything
         * left stays queued; release_room() will keep the room ready so it gets
         * another turn. */
        message_t *m = r->q_head;
        int delivered = 0;
        while (m != NULL && delivered < SERVE_QUANTUM) {
            char line[NAME_LEN + NAME_LEN + BUF_LEN + 16];
            snprintf(line, sizeof(line), "[%s] %s: %s\n",
                     r->name, m->sender, m->text);

            /* Send to every current member of the room. */
            for (int i = 0; i < MAX_MEMBERS; i++) {
                int cid = r->members[i];
                if (cid == -1)
                    continue;
                int fd = fd_for_client(cid);   /* -1 if that member left */
                if (fd >= 0)
                    send_line(fd, line);
            }

            /* Persist it: hand the record to the logger via the FIFO. This is
             * non-blocking and drops the record if the logger can't keep up,
             * so it never stalls delivery. */
            log_message(r->name, m->sender, m->text);

            message_t *next = m->next;
            free(m);
            m = next;
            r->q_len--;
            delivered++;
        }
        /* Reattach the remaining tail (if any). */
        r->q_head = m;
        if (m == NULL)
            r->q_tail = NULL;

        pthread_mutex_unlock(&r->lock);

        printf("Worker %d delivered %d message(s) to room '%s' (id=%d)\n",
               worker_id, delivered, r->name, r->id);
        fflush(stdout);

        release_room(r);                    /* back to the scheduler */
    }
    return NULL;    /* not reached */
}

/* ------------------------------------------------------------------ */
/* Per-client thread                                                  */
/* ------------------------------------------------------------------ */

/*
 * Handle a single, complete command line (no trailing newline) from client `c`.
 * Empty lines are ignored. Any reply is written straight back to the client.
 */
static void handle_command(client_t *c, const char *line)
{
    char out[BUF_LEN + 64];

    if (line[0] == '\0')
        return;                 /* empty line, ignore */

    /* --- Command: JOIN <room_name> --- */
    if (strncmp(line, "JOIN ", 5) == 0) {
        const char *room = line + 5;
        while (*room == ' ')          /* skip extra spaces */
            room++;

        if (*room == '\0') {
            send_line(c->fd, "ERR JOIN requires a room name\n");
            return;
        }

        /* Create the room if needed, then add this client as a member. */
        room_t *r = find_or_create_room(room);
        if (r == NULL) {
            send_line(c->fd, "ERR room table full\n");
            return;
        }
        if (room_add_member(r, c->id) != 0) {
            send_line(c->fd, "ERR room is full\n");
            return;
        }

        /* Record the client's current room under the client mutex. */
        pthread_mutex_lock(&clients_mutex);
        c->room_id = r->id;
        pthread_mutex_unlock(&clients_mutex);

        snprintf(out, sizeof(out), "OK joined %s\n", r->name);
        send_line(c->fd, out);
    }
    /* --- Command: PRIORITY <room_name> <n> (admin only) --- */
    else if (strncmp(line, "PRIORITY ", 9) == 0) {
        if (!c->is_admin) {
            send_line(c->fd, "ERR PRIORITY is admin-only\n");
            return;
        }

        char rname[NAME_LEN];
        int prio;
        /* Parse exactly "<room> <n>"; %31s bounds the room name. */
        if (sscanf(line + 9, "%31s %d", rname, &prio) != 2) {
            send_line(c->fd, "ERR usage: PRIORITY <room_name> <n>\n");
            return;
        }
        if (prio < 1) {
            send_line(c->fd, "ERR priority must be >= 1\n");
            return;
        }

        room_t *r = find_room_by_name(rname);
        if (r == NULL) {
            send_line(c->fd, "ERR no such room\n");
            return;
        }

        set_room_priority(r, prio);
        printf("Admin %s set room '%s' (id=%d) priority to %d\n",
               c->name, r->name, r->id, prio);
        fflush(stdout);

        snprintf(out, sizeof(out), "OK priority %s = %d\n", r->name, prio);
        send_line(c->fd, out);
    }
    /* --- Anything else: a chat message to enqueue in the client's room --- */
    else {
        if (c->room_id < 0) {
            send_line(c->fd, "ERR join a room first (JOIN <room>)\n");
            return;
        }

        room_t *r = find_room_by_id(c->room_id);
        if (r == NULL) {
            send_line(c->fd, "ERR your room no longer exists\n");
            return;
        }

        long qlen = room_enqueue(r, c->name, line);
        if (qlen < 0) {
            send_line(c->fd, "ERR could not queue message\n");
            return;
        }

        /* Enqueued: log it, then flag the room so a worker delivers it. */
        printf("Queued message in room '%s' (id=%d) from %s: \"%s\" "
               "(%ld now queued)\n",
               r->name, r->id, c->name, line, qlen);
        fflush(stdout);

        mark_room_ready(r);         /* hand it to the worker pool */
        send_line(c->fd, "OK queued\n");
    }
}

static void *client_thread(void *arg)
{
    client_t *c = (client_t *)arg;
    char rbuf[BUF_LEN];       /* raw bytes from one read()                     */
    char line[BUF_LEN];       /* one command line reassembled across reads     */
    size_t line_len = 0;      /* bytes currently buffered in `line`            */
    int overflow = 0;         /* set when the current line exceeds `line`      */
    char out[BUF_LEN + 64];

    /* Greet the client so a human using netcat sees something. */
    snprintf(out, sizeof(out),
             "Welcome %s (id=%d)%s. Commands: JOIN <room> | <message>%s\n",
             c->name, c->id,
             c->is_admin ? " [ADMIN]" : "",
             c->is_admin ? " | PRIORITY <room> <n>" : "");
    send_line(c->fd, out);

    /*
     * Read bytes and split them into '\n'-delimited command lines. TCP is a
     * byte stream, so one read() may carry several commands, a partial command,
     * or both — we must not assume one read() == one command. Complete lines
     * are dispatched immediately; a trailing partial line is carried over to
     * the next read(). '\r' is tolerated so \r\n clients work.
     */
    for (;;) {
        ssize_t n = read(c->fd, rbuf, sizeof(rbuf));
        if (n < 0) {
            if (errno == EINTR)
                continue;      /* interrupted read, try again */
            break;             /* real error */
        }
        if (n == 0)
            break;             /* orderly disconnect (EOF) */

        for (ssize_t i = 0; i < n; i++) {
            char ch = rbuf[i];
            if (ch == '\n' || ch == '\r') {
                if (overflow) {
                    /* The line was too long; we already dropped it. Reset. */
                    send_line(c->fd, "ERR line too long\n");
                    overflow = 0;
                    line_len = 0;
                } else {
                    line[line_len] = '\0';
                    handle_command(c, line);
                    line_len = 0;
                }
            } else if (line_len < sizeof(line) - 1) {
                line[line_len++] = ch;
            } else {
                /* Overflow: mark and keep discarding until the next newline. */
                overflow = 1;
            }
        }
    }

    /* Disconnect: remove from the global list under the mutex, then clean up. */
    pthread_mutex_lock(&clients_mutex);
    remove_client(c->id);
    pthread_mutex_unlock(&clients_mutex);

    printf("Client %d (%s) disconnected.\n", c->id, c->name);
    fflush(stdout);

    close(c->fd);
    free(c);
    return NULL;
}

/* ------------------------------------------------------------------ */
/* main: set up the listening socket and accept loop                  */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv)
{
    /*
     * Args:  [port] [num_workers] [--scheduler round_robin|priority]
     * The two numbers are positional (in order); the scheduler flag may appear
     * anywhere and accepts either "--scheduler X" or "--scheduler=X".
     */
    int port        = DEFAULT_PORT;
    int num_workers = DEFAULT_WORKERS;
    const char *scheduler = "round_robin";
    int positional = 0;

    for (int i = 1; i < argc; i++) {
        const char *sched_val = NULL;
        if (strcmp(argv[i], "--scheduler") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--scheduler needs an argument\n");
                return 1;
            }
            sched_val = argv[++i];
        } else if (strncmp(argv[i], "--scheduler=", 12) == 0) {
            sched_val = argv[i] + 12;
        }

        if (sched_val != NULL) {
            scheduler = sched_val;
            continue;
        }

        /* Otherwise it is a positional argument: port, then num_workers. */
        if (positional == 0) {
            port = atoi(argv[i]);
            if (port <= 0 || port > 65535) {
                fprintf(stderr, "Invalid port: %s\n", argv[i]);
                return 1;
            }
        } else if (positional == 1) {
            num_workers = atoi(argv[i]);
            if (num_workers < 1 || num_workers > MAX_WORKERS) {
                fprintf(stderr, "Invalid worker count: %s (1..%d)\n",
                        argv[i], MAX_WORKERS);
                return 1;
            }
        } else {
            fprintf(stderr, "Unexpected argument: %s\n", argv[i]);
            return 1;
        }
        positional++;
    }

    /* Bind the chosen scheduler strategy. The worker loop is unaffected. */
    if (strcmp(scheduler, "round_robin") == 0) {
        select_serveable_locked = select_rr_locked;
    } else if (strcmp(scheduler, "priority") == 0) {
        select_serveable_locked = select_priority_locked;
    } else {
        fprintf(stderr, "Unknown scheduler '%s' (use round_robin|priority)\n",
                scheduler);
        return 1;
    }
    printf("Scheduler: %s\n", scheduler);
    fflush(stdout);

    /* Writing to a FIFO whose reader has gone away raises SIGPIPE, which would
     * kill us by default. Ignore it so a dead logger surfaces as EPIPE from
     * write() instead — handled gracefully in log_message(). */
    signal(SIGPIPE, SIG_IGN);

    /* Create the FIFO the logger reads from. Harmless if it already exists. */
    if (mkfifo(FIFO_PATH, 0666) < 0 && errno != EEXIST) {
        perror("mkfifo");           /* non-fatal: chat works, persistence won't */
    } else {
        printf("FIFO ready at %s (start ./logger to persist history)\n",
               FIFO_PATH);
        fflush(stdout);
    }

    /* Start the delivery worker pool. Deliberately small: with fewer workers
     * than rooms, some ready rooms must wait, which the per-worker logs show. */
    pthread_t workers[MAX_WORKERS];
    for (int i = 0; i < num_workers; i++) {
        if (pthread_create(&workers[i], NULL,
                           worker_thread, (void *)(intptr_t)i) != 0) {
            perror("pthread_create (worker)");
            return 1;
        }
        pthread_detach(workers[i]);
    }
    printf("Started %d delivery worker(s)\n", num_workers);
    fflush(stdout);

    /* Create the listening TCP socket. */
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket");
        return 1;
    }

    /* Allow quick restarts without "Address already in use". */
    int yes = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
        perror("setsockopt");   /* non-fatal */

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);   /* listen on all interfaces */
    addr.sin_port        = htons((uint16_t)port);

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(listen_fd);
        return 1;
    }

    if (listen(listen_fd, 16) < 0) {
        perror("listen");
        close(listen_fd);
        return 1;
    }

    printf("Chat server listening on port %d\n", port);
    fflush(stdout);

    /* Accept loop: one detached thread per client. */
    for (;;) {
        struct sockaddr_in cli_addr;
        socklen_t cli_len = sizeof(cli_addr);
        int fd = accept(listen_fd, (struct sockaddr *)&cli_addr, &cli_len);
        if (fd < 0) {
            if (errno == EINTR)
                continue;
            perror("accept");
            continue;           /* keep serving other clients */
        }

        /* Allocate and populate the client record. */
        client_t *c = calloc(1, sizeof(client_t));
        if (c == NULL) {
            perror("calloc");
            close(fd);
            continue;
        }
        c->fd      = fd;
        c->room_id = -1;

        /* Register the client (assign id, default name, add to list). The very
         * first client to connect becomes the admin. */
        pthread_mutex_lock(&clients_mutex);
        c->id = next_client_id++;
        snprintf(c->name, sizeof(c->name), "client%d", c->id);
        if (!admin_assigned) {
            c->is_admin  = 1;
            admin_assigned = 1;
        }
        int rc = add_client(c);
        pthread_mutex_unlock(&clients_mutex);

        if (rc != 0) {
            /* Server full: politely refuse and clean up. */
            send_line(fd, "ERR server full, try again later\n");
            close(fd);
            free(c);
            continue;
        }

        printf("Client %d connected from %s:%d\n",
               c->id,
               inet_ntoa(cli_addr.sin_addr),
               ntohs(cli_addr.sin_port));
        fflush(stdout);

        /* Spawn a detached thread so its resources are reclaimed on exit. */
        pthread_t tid;
        if (pthread_create(&tid, NULL, client_thread, c) != 0) {
            perror("pthread_create");
            pthread_mutex_lock(&clients_mutex);
            remove_client(c->id);
            pthread_mutex_unlock(&clients_mutex);
            close(fd);
            free(c);
            continue;
        }
        pthread_detach(tid);
    }

    /* Not reached, but tidy for completeness. */
    close(listen_fd);
    return 0;
}
