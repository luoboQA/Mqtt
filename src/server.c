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

#include "server_internal.h"
#include "acl.h"
#include "config.h"
#include "ev.h"
#include "handlers.h"
#include "logging.h"
#include "memory.h"
#include "memorypool.h"
#include "network.h"
#include "persistence.h"
#include "sol_internal.h"
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>

/* Protects broker maps shared by event-loop threads. */
pthread_mutex_t mutex;

/*
 * Auxiliary structure to be used as init argument for eventloop, fd is the
 * listening socket we want to share between multiple instances, cronjobs is
 * just a flag to signal if we want to register cronjobs on that particular
 * instance or not (to not repeat useless cron jobs on multiple threads)
 */
struct listen_payload {
    int fd;
    bool cronjobs;
    struct server *server;
    /*
     * Slot this loop owns in the `loops` array below. Every thread gets a
     * payload of its own: they used to share one struct, and a worker could
     * read `cronjobs` after the main thread had already set it, registering a
     * second (and third) copy of every periodic task
     */
    int index;
};

/*
 * The event contexts of every loop, filled in when each thread starts and
 * disposed of once every thread has left its own (see eventloop_start and the
 * end of start_server): a loop that is stopping can still be the target of
 * another one (a delivery, a takeover), so nothing may be released until all
 * of them are done
 */
static struct ev_ctx *loops[THREADSNR + 1];

/*
 * General informations of the broker, all fields will be published
 * periodically to internal topics
 */
struct sol_info info;

/*
 * Stop callback used during shutdown to wake and exit a running event loop.
 */
static void stop_handler(struct ev_ctx *ctx, void *arg)
{
    (void)arg;
    ev_stop(ctx);
}

/*
 * Start one event loop. Each loop owns its accepted clients; periodic jobs are
 * registered only on loop zero to avoid running the same maintenance twice.
 */
static void eventloop_start(void *args)
{
    struct listen_payload *loop_data = args;
    struct server *server            = loop_data->server;
    struct config *conf              = server->conf;
    struct ev_ctx *ctx               = try_alloc(sizeof(*ctx));
    int sfd                          = loop_data->fd;
    loops[loop_data->index]          = ctx;
    ev_init(ctx, EVENTLOOP_MAX_EVENTS);
    ctx->data = server;
    /*
     * Register the shared run descriptor: on Linux it is the eventfd, on
     * other platforms the *read* end of the pipe (index 0). The write end
     * (index 1) is what the signal handler writes to, and registering it
     * here meant the loops waited for a write end to become readable, so
     * the shutdown event never arrived and the broker had to be killed
     */
#ifdef __linux__
    ev_register_event(ctx, conf->run, EV_CLOSEFD | EV_READ, stop_handler,
                      NULL);
#else
    ev_register_event(ctx, conf->run[0], EV_CLOSEFD | EV_READ, stop_handler,
                      NULL);
#endif
    // Register listening FD with accept callback
    ev_register_event(ctx, sfd, EV_READ, client_accept_callback, &sfd);
    /*
     * Periodic tasks run on one loop only, the one start_server() starts on
     * its own thread: every thread has its own payload now, so a worker can
     * no longer catch a flag the main thread set after starting it
     */
    if (loop_data->cronjobs == true) {
        ev_register_cron(ctx, cron_publish_stats, NULL, conf->stats_pub_interval,
                         0);
        ev_register_cron(ctx, cron_inflight_msg_check, NULL, 1, 0);
        ev_register_cron(ctx, cron_keepalive_check, NULL, 1, 0);
        ev_register_cron(ctx, cron_persistence_check, NULL, 1, 0);
        ev_register_cron(ctx, cron_memory_check, NULL, 1, 0);
    }
    // Start the loop, blocking call
    ev_run(ctx);
    /*
     * The context is deliberately *not* destroyed here: another loop may
     * still be about to fire an event at it (a delivery to one of its
     * clients, a takeover), and walking a freed array is exactly the
     * use-after-free the sanitizer found at shutdown. start_server() disposes
     * of every context once all the loops have returned
     */
}

/*
 * ===================
 *  Main APIs exposed
 * ===================
 */

/* Start broker state, the listener, and its event-loop threads. */
int start_server(struct server *server, const char *addr, const char *port)
{
    struct config *conf = server->conf;

    INIT_INFO;

    /* Initialize state owned by this broker context. */
    server->store = topic_store_new();
    server->auths = NULL;
    server->pool  = memorypool_new(BASE_CLIENTS_NUM, sizeof(struct client));
    if (!server->pool)
        log_fatal("Failed to allocate %d sized memory pool for clients",
                  BASE_CLIENTS_NUM);
    log_info("Client pool: %d blocks of %zu bytes, grown on demand",
             BASE_CLIENTS_NUM, sizeof(struct client));
    server->clients_map = NULL;
    server->sessions    = NULL;
    server->over_memory = false;
    pthread_mutex_init(&mutex, NULL);

    if (conf->allow_anonymous == false)
        if (!config_read_passwd_file(conf->password_file, &server->auths))
            log_error("Failed to read password file");

    cron_topics_init(server);

    /*
     * Topic level authorization: an access control list is read before the
     * listening socket exists, so no client can be served before the rules
     * are in place
     */
    acl_load(conf->acl_file);

    /*
     * Rebuild what a previous run left behind: the retained messages and the
     * persistent sessions. This happens before the listening socket exists,
     * so no client can race it, and persistence_load() takes no lock for the
     * same reason
     */
    persistence_load(server, conf->persistence_file);

    /*
     * A limit below what the broker already holds would refuse every new
     * connection from the first second, which looks like a hang from the
     * outside: warn about it rather than let it surprise anybody
     */
    if (conf->max_memory > 0 && conf->max_memory <= memory_used())
        log_warning("max_memory (%zu bytes) is below the memory the broker "
                    "already uses (%zu bytes): raise it, or new connections "
                    "will be refused",
                    conf->max_memory, memory_used());

    /* Start listening for new connections */
    int sfd = make_listen(addr, port, conf->socket_family, conf);

    /* Setup SSL in case of flag true */
    if (conf->tls == true) {
        openssl_init();
        server->ssl_ctx = create_ssl_context(conf);
        load_certificates(server->ssl_ctx, conf->cafile, conf->certfile,
                          conf->keyfile, conf->require_client_cert);
    }

    log_info("Server start");
    info.start_time                  = time(NULL);

    struct listen_payload main_loop = {.fd      = sfd,
                                       .cronjobs = true,
                                       .server   = server,
                                       .index    = THREADSNR};

#if THREADSNR > 0
    pthread_t thrs[THREADSNR];
    /* One payload per thread, alive until the joins below */
    struct listen_payload worker_loops[THREADSNR];
    for (int i = 0; i < THREADSNR; ++i) {
        worker_loops[i] = (struct listen_payload){.fd      = sfd,
                                                  .cronjobs = false,
                                                  .server   = server,
                                                  .index    = i};
        pthread_create(&thrs[i], NULL, (void *(*)(void *)) & eventloop_start,
                       &worker_loops[i]);
    }
#endif
    // start eventloop, could be spread on multiple threads
    eventloop_start(&main_loop);

#if THREADSNR > 0
    for (int i = 0; i < THREADSNR; ++i)
        pthread_join(thrs[i], NULL);
#endif

    /*
     * Every loop has returned by now, so no other thread can reach these
     * contexts anymore: this is the point where they can be released
     */
    for (int i = 0; i <= THREADSNR; ++i) {
        if (!loops[i])
            continue;
        ev_destroy(loops[i]);
        free_memory(loops[i]);
        loops[i] = NULL;
    }

    /*
     * No event loop can touch a client now. Close remaining sockets and
     * release buffers before snapshotting; persistent sessions stay mapped
     * until the final snapshot has been written.
     */
    client_shutdown_all(server);

    /*
     * Final snapshot, catching whatever the last periodic run did not: every
     * loop has stopped by now, so there is no lock left to take
     */
    if (conf->persistence_file[0] && persistence_is_dirty())
        persistence_save(server, conf->persistence_file);

    close(sfd);
    AUTH_DESTROY(server->auths);
    acl_destroy();
    topic_store_destroy(server->store);
    session_destroy_all(server);
    memorypool_destroy(server->pool);

    /* Destroy SSL context, if any present */
    if (conf->tls == true) {
        SSL_CTX_free(server->ssl_ctx);
        openssl_cleanup();
    }
    pthread_mutex_destroy(&mutex);

    log_info("Sol v%s exiting", VERSION);

    return SOL_OK;
}

/*
 * Make the entire process a daemon running in background
 */
void daemonize(void)
{

    int fd;

    if (fork() != 0)
        exit(0);

    setsid();

    if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO)
            close(fd);
    }
}
