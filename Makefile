# Makefile for the multi-threaded TCP chat server and its logger process.
#
#   make          - build ./chat_server and ./logger
#   make run      - build and run the server on port 5000 with 3 workers
#   make logger   - build only the logger
#   make clean    - remove build artifacts and the runtime FIFO
#                   (chat_history.log is kept)

CC      := cc
CFLAGS  := -Wall -Wextra -O2 -pthread
LDFLAGS := -pthread          # link pthread (needed on some toolchains)

SERVER  := chat_server
LOGGER  := logger
FIFO    := chat_fifo

.PHONY: all run clean

all: $(SERVER) $(LOGGER)

$(SERVER): chat_server.c
	$(CC) $(CFLAGS) -o $@ chat_server.c $(LDFLAGS)

# The logger is single-threaded and needs no pthread linkage.
$(LOGGER): logger.c
	$(CC) $(CFLAGS) -o $@ logger.c

run: $(SERVER)
	./$(SERVER) 5000 3

clean:
	rm -f $(SERVER) $(LOGGER) $(FIFO)
