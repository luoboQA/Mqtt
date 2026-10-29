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

#ifndef EV_H
#define EV_H

#include <pthread.h>
#include <time.h>

#define EV_OK  0
#define EV_ERR 1
#define EV_OOM 2

/*
 * Event types, meant to be OR-ed on a bitmask to define the type of an event
 * which can have multiple traits
 */
enum ev_type {
    EV_NONE       = 0x00,
    EV_READ       = 0x01,
    EV_WRITE      = 0x02,
    EV_DISCONNECT = 0x04,
    EV_EVENTFD    = 0x08,
    EV_TIMERFD    = 0x10,
    EV_CLOSEFD    = 0x20
};

struct ev_ctx;

/*
 * Event struture used as the main carrier of clients informations, it will be
 * tracked by an array in every context created
 */
struct ev {
    int fd;
    int mask;
    void *rdata; // opaque pointer for read callback args
    void *wdata; // opaque pointer for write callback args
    void (*rcallback)(struct ev_ctx *, void *); // read callback
    void (*wcallback)(struct ev_ctx *, void *); // write callback
};

/*
 * Event loop context, carry the expected number of events to be monitored at
 * every cycle and an opaque pointer to the backend used as engine
 * (Epoll, with Poll/Select/Kqueue fallbacks for platforms that lack it).
 *
 * The server runs one loop per thread: every eventloop_start() builds its own
 * ev_ctx and, on the epoll backend, its own epoll descriptor, so a loop is
 * owned by a single thread. Registrations are level-triggered (no EPOLLET and
 * no EPOLLONESHOT): a readable or writable descriptor stays ready until it is
 * drained or removed. A worker thread that must reply on a client owned by
 * another loop arms it cross-thread through ev_fire_event(), which writes the
 * target slot and issues an epoll_ctl() MOD against that loop's descriptor.
 */
struct ev_ctx {
    int events_nr;
    int maxfd; // the highest descriptor monitored by this context on the
               // select backend, so events_monitored needs maxfd + 1 slots
               // to be addressable by fd (the epoll/kqueue backends address
               // through their own kernel object instead)
    int stop;
    int maxevents;
    /*
     * Capacity of the events_monitored array, i.e. the number of slots it
     * holds. It is tracked separately from events_nr/maxevents because those
     * two are also handed to the backend (epoll_wait/kevent) as the size of
     * *its* events array, which is allocated once at init and never grown
     */
    int monitored_nr;
    unsigned long long fired_events;
    struct ev *events_monitored;
    /*
     * Guards the monitored array above. The loop that owns this context reads
     * it while dispatching, and other loops write to it (a publish towards a
     * client living here, a takeover, the keepalive scan fire an event at
     * it), which the thread sanitizer flags as a data race on the callback
     * pointers and on the reallocation of the array. Never held across a
     * callback or the blocking wait: only while the slot is read or written
     */
    pthread_mutex_t lock;
    /*
     * Descriptor a loop watches so another thread can pull it out of its
     * blocking wait: poll() and select() copy the descriptor set on entry, so
     * a rearm done from the outside is invisible to a wait in progress.
     * Written to by ev_fire_event() (the backends that need it, see
     * EV_BACKEND_NEEDS_WAKEUP), drained by the callback watching it
     */
    int wakeup;
    void *api; // opaque pointer to platform defined backends
    /*
     * Opaque pointer to whatever the loop owner needs to reach from a
     * callback. The broker stores its struct server * here, so a callback
     * never has to fall back on a process wide global
     */
    void *data;
};

int ev_init(struct ev_ctx *, int);

void ev_destroy(struct ev_ctx *);

/*
 * Poll an event context for events, accepts a timeout or block forever,
 * returning only when a list of FDs are ready to either READ, WRITE or TIMER
 * to be executed.
 */
int ev_poll(struct ev_ctx *, time_t);

/*
 * Blocks forever in a loop polling for events with ev_poll calls. At every
 * cycle executes callbacks registered with each event
 */
int ev_run(struct ev_ctx *);

/*
 * Trigger a stop on a running event, it's meant to be run as an event in a
 * running ev_ctx
 */
void ev_stop(struct ev_ctx *);

/*
 * Add a single FD to the underlying backend of the event loop. Equal to
 * ev_fire_event just without an event to be carried. Useful to add simple
 * descritors like a listening socket o message queue FD.
 */
int ev_watch_fd(struct ev_ctx *, int, int);

/*
 * Remove a FD from the loop, even tho a close syscall is sufficient to remove
 * the FD from the underlying backend such as EPOLL/SELECT, this call ensure
 * that any associated events is cleaned out an set to EV_NONE
 */
int ev_del_fd(struct ev_ctx *, int);

/*
 * Register a new event, semantically it's equal to ev_register_event but
 * it's meant to be used when an FD is not already watched by the event loop.
 * It could be easily integrated in ev_fire_event call but I prefer maintain
 * the samantic separation of responsibilities.
 */
int ev_register_event(struct ev_ctx *, int, int,
                      void (*callback)(struct ev_ctx *, void *), void *);

int ev_register_cron(struct ev_ctx *, void (*callback)(struct ev_ctx *, void *),
                     void *, long long, long long);

/*
 * Register a new event for the next loop cycle to a FD. Equal to ev_watch_fd
 * but allow to carry an event object for the next cycle.
 */
int ev_fire_event(struct ev_ctx *, int, int,
                  void (*callback)(struct ev_ctx *, void *), void *);

#endif
