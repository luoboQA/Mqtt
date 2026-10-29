/* BSD 2-Clause License
 *
 * Copyright (c) 2023, Andrea Giacomo Baldan All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef SERVER_H
#define SERVER_H

#include "mqtt.h"
#include <openssl/ssl.h>

struct config;

/*
 * Number of worker threads to be created. Each one will host his own ev_ctx
 * loop. This doesn't take into account the main thread, so to know the total
 * number of running loops +1 must be added to the THREADSNR value.
 */
#define THREADSNR            2

/*
 * Epoll default settings for concurrent events monitored and timeout, -1
 * means no timeout at all, blocking undefinitely
 */
#define EVENTLOOP_MAX_EVENTS 1024
#define EVENTLOOP_TIMEOUT    -1

/* Initial chunk of client blocks allocated at start-up. The pool appends a
 * new chunk of the same size whenever the free list runs dry (see
 * memorypool_alloc()), so the broker no longer pays for the worst case it
 * will never reach: the old fixed 128K-block chunk cost ~56MB before a
 * single client connected, which is what made max_memory meaningless
 */
#define BASE_CLIENTS_NUM     1024

struct server;

/*
 * IO event structure, the argument handed to the command handlers. It pairs
 * the client the bytes arrived on with the decoded packet: process_message()
 * builds one on the stack, mqtt_unpack() fills `data`, handle_command()
 * dispatches on the type, and the handler writes its reply straight into that
 * client's write buffer. There is no separate worker thread pool nor a second
 * epoll to hand the request to: the handler runs on the very loop thread that
 * read the packet
 */
struct io_event {
    struct client *client;
    struct server *server;
    struct mqtt_packet data;
};

/* Global informations statistics structure */
struct sol_info {
    /* Number of clients currently connected */
    atomic_size_t active_connections;
    /* Total number of clients connected since the start */
    atomic_size_t total_connections;
    /* Total number of sent messages */
    atomic_size_t messages_sent;
    /* Total number of received messages */
    atomic_size_t messages_recv;
    /* Timestamp of the start time */
    atomic_size_t start_time;
    /* Seconds passed since the start */
    atomic_size_t uptime;
    /* Total number of bytes received */
    atomic_size_t bytes_recv;
    /* Total number of bytes sent out */
    atomic_size_t bytes_sent;
    /* Messages dropped by the memory policy (oldest offline queue entries) */
    atomic_size_t messages_dropped;
};

#define INIT_INFO                                                              \
    do {                                                                       \
        info.active_connections = ATOMIC_VAR_INIT(0);                          \
        info.total_connections  = ATOMIC_VAR_INIT(0);                          \
        info.messages_sent      = ATOMIC_VAR_INIT(0);                          \
        info.messages_recv      = ATOMIC_VAR_INIT(0);                          \
        info.start_time         = ATOMIC_VAR_INIT(0);                          \
        info.uptime             = ATOMIC_VAR_INIT(0);                          \
        info.bytes_sent         = ATOMIC_VAR_INIT(0);                          \
        info.bytes_recv         = ATOMIC_VAR_INIT(0);                          \
        info.messages_dropped   = ATOMIC_VAR_INIT(0);                          \
    } while (0)

/*
 * General informations of the broker, all fields will be published
 * periodically to internal topics
 */
extern struct sol_info info;

/*
 * Main structure, a global instance will be instantiated at start, tracking
 * topics, connected clients and registered closures.
 *
 * pending_msgs and pendings_acks are two arrays used to track remaining
 * messages to push out and acks respectively.
 */
struct server {
    /*
     * The configuration this broker runs with: owned by main() and only read
     * after startup, so every module gets it through the server instead of a
     * process wide global
     */
    struct config *conf;
    // The main topics store
    struct topic_store *store;
    // A memory pool for clients allocation
    struct memorypool *pool;
    // Our clients map, it's a handle pointer for UTHASH APIs, must be set to
    // NULL
    struct client *clients_map;
    // The global session map, another UTHASH handle pointer, must be set to
    // NULL
    struct client_session *sessions;
    // UTHASH handle pointer for authentications
    struct authentication *auths;
    /*
     * Set while the broker is over its max_memory and has nothing left to
     * give back: new connections are refused until it drops again. Written
     * by the memory_check() periodic task, read on every accept
     */
    volatile atomic_bool over_memory;
    // Application TLS context
    SSL_CTX *ssl_ctx;
};


/*
 * Main entry point for the server, to be called with an address and a port
 * to start listening. The function may fail only in the case of Out of memory
 * error occurs or listen call fails, on the other cases it should just log
 * unexpected errors.
 */
int start_server(struct server *, const char *, const char *);

/*
 * Fire a write callback to reply after a client request, under the hood it
 * schedules an EV_WRITE event with a client pointer set to write carried
 * contents out on the socket descriptor.
 */
void enqueue_event_write(const struct client *);

/*
 * Disconnect any live connection registered under the given client id: MQTT
 * v3.1.1 §3.1.4 requires the server to drop the older connection when a new
 * CONNECT carries an id already in use. The caller must hold the global
 * server mutex, `keep` is the connection taking the id over (skipped if it
 * somehow matches the registered one)
 */
/*
 * Disconnect the client currently holding `client_id`, if any, as MQTT
 * v3.1.1 §3.1.4-2 requires when a second connection takes that id over.
 * When the victim had a will, a deep copy of it is left in `will`: its
 * connection is closed by the server without a DISCONNECT, so §3.1.2.5 wants
 * it published. The caller owns the copy and releases it with
 * mqtt_packet_destroy() (publish_lwt() takes the global mutex, which the
 * caller holds here, so the delivery has to happen after unlocking)
 */
void server_kick_duplicate(const char *, const struct client *,
                           struct mqtt_packet *will);

/*
 * Make the entire process a daemon running in background
 */
void daemonize(void);

#endif
