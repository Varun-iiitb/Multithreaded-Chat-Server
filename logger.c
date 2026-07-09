/*
 * logger.c — persistence process for the chat server.
 *
 * Reads records from the named pipe (FIFO) that the chat server writes to and
 * appends each one, with a timestamp, to chat_history.log. Keeping all disk
 * I/O in this separate process means the server never blocks on the disk.
 *
 * Record format on the FIFO (one per line):  <room>|<sender>|<text>
 * Line written to the history file:          [YYYY-MM-DD HH:MM:SS] <room>|<sender>|<text>
 *
 * The logger is resilient to the server restarting: when every writer closes
 * the FIFO we read EOF, then loop back and reopen it to wait for the next
 * writer. Opening the FIFO for reading blocks until a writer appears, so an
 * idle logger consumes no CPU.
 *
 * Build:  make          (built alongside chat_server)
 * Run:    ./logger
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>

#define FIFO_PATH     "chat_fifo"
#define HISTORY_FILE  "chat_history.log"
#define LINE_MAX_LEN  2048

int main(void)
{
    /* Make sure the FIFO exists. The server also creates it; whichever process
     * starts first wins and the other's mkfifo harmlessly reports EEXIST. */
    if (mkfifo(FIFO_PATH, 0666) < 0 && errno != EEXIST) {
        perror("mkfifo");
        return 1;
    }

    /* Open the history file once, in append mode, so restarts never truncate
     * previously logged messages. */
    FILE *hist = fopen(HISTORY_FILE, "a");
    if (hist == NULL) {
        perror("fopen history");
        return 1;
    }

    printf("Logger started; appending to %s\n", HISTORY_FILE);
    fflush(stdout);

    char line[LINE_MAX_LEN];

    /*
     * Outer loop: (re)open and drain the FIFO. fopen("r") blocks until the
     * server opens the write end, so waiting is free. When all writers close,
     * fgets() returns NULL (EOF) and we come back here to await the next one.
     */
    for (;;) {
        FILE *fifo = fopen(FIFO_PATH, "r");
        if (fifo == NULL) {
            perror("fopen fifo");
            break;
        }
        printf("Logger: server connected.\n");
        fflush(stdout);

        while (fgets(line, sizeof(line), fifo) != NULL) {
            /* Drop the trailing newline(s) so our formatting stays clean. */
            size_t n = strlen(line);
            while (n > 0 && (line[n - 1] == '\n' || line[n - 1] == '\r'))
                line[--n] = '\0';
            if (n == 0)
                continue;

            /* Timestamp the record. */
            time_t now = time(NULL);
            struct tm tmv;
            char ts[32];
            localtime_r(&now, &tmv);
            strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tmv);

            /* Append and flush immediately so history survives a crash. */
            fprintf(hist, "[%s] %s\n", ts, line);
            fflush(hist);
        }

        /* EOF: the server closed the FIFO (shut down or restarted). */
        printf("Logger: server disconnected; waiting for reconnect.\n");
        fflush(stdout);
        fclose(fifo);
    }

    fclose(hist);
    return 0;
}
