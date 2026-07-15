# Multithreaded Chat Server

A concurrent, room-based TCP chat server written in C using POSIX sockets and
pthreads, with message persistence handled by a separate logger process over a
named pipe (FIFO). Built as a systems/OS project to explore concurrency,
synchronization, IPC, and scheduling.

## Features

- **Multithreaded TCP server** — configurable port; one detached `pthread` per
  connected client.
- **Rooms with per-room message queues** — clients `JOIN` rooms; each room has
  its own FIFO message queue guarded by its own mutex, so different rooms never
  contend.
- **Worker thread pool** — a small, configurable pool of worker threads
  (intentionally fewer than the number of rooms) delivers queued messages,
  making scheduling contention observable.
- **Condition-variable scheduling (no busy-waiting)** — idle workers block on a
  condition variable and consume zero CPU until a message is enqueued; a
  predicate loop handles spurious wakeups correctly.
- **Swappable scheduler** — choose `--scheduler round_robin` (equal turns) or
  `--scheduler priority` (smooth weighted round-robin). Under contention, a room
  of priority *N* is served *N* times as often as a priority-1 room (verified at
  ~5:1 for weight 5).
- **Runtime priority control** — a designated admin client can change a room's
  priority live with `PRIORITY <room> <n>`.
- **Crash-resilient persistence** — a separate `logger` process reads a named
  pipe and appends every message to `chat_history.log` with a timestamp. The
  server writes to the FIFO non-blocking (ignoring `SIGPIPE`), so it never
  stalls on disk I/O and survives the logger disconnecting and reconnecting.
- **Thread-safe throughout** — global client and room lists, per-room queues,
  the ready-room set, and the log writer are each protected by dedicated
  mutexes with a consistent lock ordering to avoid deadlock.

## Architecture

```
client ── TCP ──►  per-client thread ──► room message queue ──► ready-room set
                                                                      │
                                                          (condition variable)
                                                                      ▼
                                                             worker thread pool
                                                                      │
                                              deliver to members  ◄───┤
                                                                      │
                                                       write "<room>|<sender>|<text>"
                                                                      ▼
                                                        named pipe (FIFO)
                                                                      ▼
                                                        logger process ──► chat_history.log
```

## Build

Requires a POSIX environment (Linux, macOS, or WSL) and a C compiler.

```sh
make          # builds ./chat_server and ./logger
```

## Run

```sh
./logger &                                          # start persistence (any time)
./chat_server <port> <num_workers> [--scheduler round_robin|priority]
# example:
./chat_server 5000 3 --scheduler priority
```

Then connect clients with any TCP tool, e.g.:

```sh
nc localhost 5000
```

The **first client to connect is the admin** (the only one allowed to run
`PRIORITY`).

## Commands

| Command | Description |
|---|---|
| `JOIN <room>` | Create the room if needed and join it. |
| `<any text>` | Send a message to your current room. |
| `PRIORITY <room> <n>` | (admin only) Set a room's scheduling weight at runtime. |

## Files

| File | Purpose |
|---|---|
| `chat_server.c` | The chat server: sockets, client threads, rooms, worker pool, scheduler, FIFO writer. |
| `logger.c` | Standalone persistence process that drains the FIFO into `chat_history.log`. |
| `Makefile` | Builds both binaries. |
