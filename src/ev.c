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

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#else
#include <sys/socket.h>
#endif
#include "config.h"
#include "ev.h"
#include "memory.h"
#include "util.h"

/*
 * ==================
 *  Wakeup descriptor
 * ==================
 *
 * poll() and select() copy the descriptor set they are given when they enter
 * the kernel, so a descriptor rearmed from another thread (a publish towards
 * a client living on this loop, a takeover, the keepalive scan) is invisible
 * to a wait already in progress: the thread would stay parked and the client
 * would never see its bytes. Every such loop watches a wakeup descriptor of
 * its own and rearms from the outside write to it. epoll does not need it:
 * epoll_ctl() is a system call the kernel applies to a wait in progress.
 */
#if defined(EPOLL)
#define EV_BACKEND_NEEDS_WAKEUP 0
#else
#define EV_BACKEND_NEEDS_WAKEUP 1
#endif

#if EV_BACKEND_NEEDS_WAKEUP

#if defined(__linux__)
#include <sys/eventfd.h>

static int ev_wakeup_create(void)
{
    return eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
}

static void ev_wakeup_notify(int fd)
{
    if (fd < 0)
        return;
    uint64_t one = 1;
    (void)!write(fd, &one, sizeof(one));
}

static void ev_wakeup_drain(int fd)
{
    uint64_t value;
    (void)!read(fd, &value, sizeof(value));
}
#else
/* A self-pipe is the portable equivalent of the eventfd above */
static int ev_wakeup_create(void)
{
    int fds[2];
    if (pipe(fds) != 0)
        return -1;
    fcntl(fds[0], F_SETFL, fcntl(fds[0], F_GETFL, 0) | O_NONBLOCK);
    fcntl(fds[1], F_SETFL, fcntl(fds[1], F_GETFL, 0) | O_NONBLOCK);
    close(fds[1]); /* Only the read end is watched */
    return fds[0];
}

static void ev_wakeup_notify(int fd)
{
    char byte = 'w';
    if (fd >= 0)
        (void)!write(fd, &byte, sizeof(byte));
}

static void ev_wakeup_drain(int fd)
{
    char buffer[64];
    while (read(fd, buffer, sizeof(buffer)) > 0)
        ;
}
#endif

/* Watched by every loop that needs one, it only has to empty the descriptor */
static void ev_wakeup_callback(struct ev_ctx *ctx, void *arg)
{
    (void)arg;
    ev_wakeup_drain(ctx->wakeup);
}

#endif /* EV_BACKEND_NEEDS_WAKEUP */

#if defined(EPOLL)

/*
 * =========================
 *  Epoll backend functions
 * =========================
 *
 * The epoll_api structure contains the epoll fd and the events array needed to
 * wait on events with epoll_wait(2) blocking call. It's the best multiplexing
 * IO api available on Linux systems and thus the optimal choice.
 */

#include <sys/epoll.h>

struct epoll_api {
    int fd;
    struct epoll_event *events;
};

/*
 * Epoll management function, register a file descriptor to an EPOLL
 * descriptor, to be monitored for read/write events
 */
static int epoll_add(int efd, int fd, int evs, void *data)
{

    struct epoll_event ev;
    ev.data.fd = fd;

    // Being ev.data a union, in case of data != NULL, fd will be set to random
    if (data)
        ev.data.ptr = data;

    ev.events = evs | EPOLLHUP | EPOLLERR;

    return epoll_ctl(efd, EPOLL_CTL_ADD, fd, &ev);
}

/*
 * Modify an epoll-monitored descriptor, can be set to EPOLLIN for read and
 * EPOLLOUT for write
 */
static int epoll_mod(int efd, int fd, int evs, void *data)
{

    struct epoll_event ev;
    ev.data.fd = fd;

    // Being ev.data a union, in case of data != NULL, fd will be set to random
    if (data)
        ev.data.ptr = data;

    ev.events = evs | EPOLLHUP | EPOLLERR;

    return epoll_ctl(efd, EPOLL_CTL_MOD, fd, &ev);
}

/*
 * Remove a descriptor from an epoll descriptor, making it no-longer monitored
 * for events
 */
static int epoll_del(int efd, int fd)
{
    return epoll_ctl(efd, EPOLL_CTL_DEL, fd, NULL);
}

static int ev_api_init(struct ev_ctx *ctx, int events_nr)
{
    struct epoll_api *e_api = try_alloc(sizeof(*e_api));
    e_api->fd               = epoll_create1(0);
    e_api->events           = try_calloc(events_nr, sizeof(struct epoll_event));
    ctx->api                = e_api;
    ctx->maxfd              = events_nr;
    return EV_OK;
}

static void ev_api_destroy(struct ev_ctx *ctx)
{
    close(((struct epoll_api *)ctx->api)->fd);
    free_memory(((struct epoll_api *)ctx->api)->events);
    free_memory(ctx->api);
}

static int ev_api_get_event_type(struct ev_ctx *ctx, int idx)
{
    struct epoll_api *e_api = ctx->api;
    int events              = e_api->events[idx].events;
    int ev_mask = ctx->events_monitored[e_api->events[idx].data.fd].mask;
    // We want to remember the previous events only if they're not of type
    // CLOSE or TIMER
    int mask    = ev_mask & (EV_CLOSEFD | EV_TIMERFD) ? ev_mask : EV_NONE;
    if (events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP))
        mask |= EV_DISCONNECT;
    if (events & EPOLLIN)
        mask |= EV_READ;
    if (events & EPOLLOUT)
        mask |= EV_WRITE;
    return mask;
}

static int ev_api_poll(struct ev_ctx *ctx, time_t timeout)
{
    struct epoll_api *e_api = ctx->api;
    return epoll_wait(e_api->fd, e_api->events, ctx->events_nr, timeout);
}

static int ev_api_watch_fd(struct ev_ctx *ctx, int fd)
{
    struct epoll_api *e_api = ctx->api;
    return epoll_add(e_api->fd, fd, EPOLLIN, NULL);
}

static int ev_api_del_fd(struct ev_ctx *ctx, int fd)
{
    struct epoll_api *e_api = ctx->api;
    return epoll_del(e_api->fd, fd);
}

static int ev_api_register_event(struct ev_ctx *ctx, int fd, int mask)
{
    struct epoll_api *e_api = ctx->api;
    int op                  = 0;
    if (mask & EV_READ)
        op |= EPOLLIN;
    if (mask & EV_WRITE)
        op |= EPOLLOUT;
    return epoll_add(e_api->fd, fd, op, NULL);
}

static int ev_api_fire_event(struct ev_ctx *ctx, int fd, int mask)
{
    struct epoll_api *e_api = ctx->api;
    int op                  = 0;
    if (mask & EV_READ)
        op |= EPOLLIN;
    if (mask & EV_WRITE)
        op |= EPOLLOUT;
    if (mask & EV_EVENTFD)
        return epoll_add(e_api->fd, fd, op, NULL);
    return epoll_mod(e_api->fd, fd, op, NULL);
}

/*
 * Get the event on the idx position inside the events map. The event can also
 * be an unset one (EV_NONE)
 */
static inline struct ev *ev_api_fetch_event(const struct ev_ctx *ctx, int idx,
                                            int mask)
{
    int fd = ((struct epoll_api *)ctx->api)->events[idx].data.fd;
    return ctx->events_monitored + fd;
}

#elif defined(POLL)

/*
 * =========================
 *  Poll backend functions
 * =========================
 *
 * The poll_api structure contains the number of fds to monitor and the array
 * of pollfd structures associated. This number must be adjusted everytime a
 * client disconnect or a new connection have an fd > nfds to avoid iterating
 * over closed fds everytime a new event is triggered.
 * As select, poll iterate linearly over all the triggered events, without the
 * hard limit of 1024 connections. It's the second best option available if no
 * epoll or kqueue for Mac OSX are not present.
 */

#include <poll.h>

struct poll_api {
    int nfds;
    int events_monitored;
    struct pollfd *fds;
};

static int ev_api_init(struct ev_ctx *ctx, int events_nr)
{
    struct poll_api *p_api  = try_alloc(sizeof(*p_api));
    p_api->nfds             = 0;
    p_api->fds              = try_calloc(events_nr, sizeof(struct pollfd));
    p_api->events_monitored = events_nr;
    ctx->api                = p_api;
    ctx->maxfd              = events_nr;
    return EV_OK;
}

static void ev_api_destroy(struct ev_ctx *ctx)
{
    struct poll_api *p_api = ctx->api;
    free_memory(p_api->fds);
    free_memory(ctx->api);
}

static int ev_api_get_event_type(struct ev_ctx *ctx, int idx)
{
    struct poll_api *p_api = ctx->api;
    /* A tombstone left by a removal: nothing to report, and its fd of -1
       would index the monitored array out of bounds */
    if (p_api->fds[idx].fd < 0)
        return EV_NONE;
    int ev_mask            = ctx->events_monitored[p_api->fds[idx].fd].mask;
    // We want to remember the previous events only if they're not of type
    // CLOSE or TIMER
    int mask               = ev_mask & (EV_CLOSEFD | EV_TIMERFD) ? ev_mask : 0;

    /*
     * revents is what poll() actually reported for this slot. The CLOSEFD
     * and TIMERFD slots above are carried over verbatim, so without this
     * guard every unrelated event would re-fire the stop handler (shutting
     * the loop down at the first connection) and the periodic crons (stats
     * and retransmissions) on each unrelated wakeup
     */
    short revents = p_api->fds[idx].revents;
    if (!(revents & (POLLIN | POLLOUT | POLLERR | POLLHUP | POLLNVAL)))
        return EV_NONE;

    if (revents & (POLLHUP | POLLERR | POLLNVAL))
        mask |= EV_DISCONNECT;
    if (revents & POLLIN)
        mask |= EV_READ;
    if (revents & POLLOUT)
        mask |= EV_WRITE;
    return mask;
}

static int ev_api_poll(struct ev_ctx *ctx, time_t timeout)
{
    struct poll_api *p_api = ctx->api;
    int err                = poll(p_api->fds, p_api->nfds, timeout);
    if (err < 0)
        return -EV_ERR;
    /*
     * Unlike epoll, poll() reports the ready descriptors by setting revents
     * on their own slots rather than compacting them at the front, so the
     * caller must walk every watched slot and let get_event_type() skip the
     * idle ones: returning the ready count here would only visit the first
     * `err` slots and miss the events reported further down the array
     */
    return p_api->nfds;
}

/*
 * Slot for a new descriptor: the first tombstone left by a removal, or a new
 * entry at the end of the array (grown if needed)
 */
static struct pollfd *ev_api_poll_slot(struct poll_api *p_api, int fd)
{
    for (int i = 0; i < p_api->nfds; ++i)
        if (p_api->fds[i].fd == -1) {
            p_api->fds[i] = (struct pollfd){.fd = fd};
            return &p_api->fds[i];
        }

    if (p_api->nfds >= p_api->events_monitored) {
        p_api->events_monitored *= 2;
        p_api->fds = try_realloc(p_api->fds, p_api->events_monitored *
                                                 sizeof(struct pollfd));
    }
    struct pollfd *slot = &p_api->fds[p_api->nfds++];
    *slot               = (struct pollfd){.fd = fd};
    return slot;
}

/*
 * Poll maintain in his state the number of file descriptor it monitor in a
 * fixed size array just like the events we monitor over the primitive. If a
 * resize is needed cause the number of fds have reached the length of the fds
 * array, we must increase its size.
 */
static int ev_api_watch_fd(struct ev_ctx *ctx, int fd)
{
    struct poll_api *p_api = ctx->api;
    struct pollfd *slot    = ev_api_poll_slot(p_api, fd);

    slot->events  = POLLIN;
    slot->revents = 0;
    return EV_OK;
}

static int ev_api_del_fd(struct ev_ctx *ctx, int fd)
{
    struct poll_api *p_api = ctx->api;
    for (int i = 0; i < p_api->nfds; ++i) {
        if (p_api->fds[i].fd == fd) {
            /*
             * The slot is left where it is, as a tombstone: poll() ignores a
             * negative descriptor, and the batch being dispatched was indexed
             * against the array as it was *before* the call. Shifting the
             * entries (as this used to) handed the remaining events of that
             * batch to the descriptors of their neighbours, and could leave
             * the cursor on a slot whose fd is -1, which then indexed
             * events_monitored[-1]
             */
            p_api->fds[i].fd      = -1;
            p_api->fds[i].events  = 0;
            p_api->fds[i].revents = 0;
            break;
        }
    }
    return EV_OK;
}

/*
 * We have to check for resize even here just like ev_api_watch_fd.
 */
static int ev_api_register_event(struct ev_ctx *ctx, int fd, int mask)
{
    struct poll_api *p_api = ctx->api;
    struct pollfd *slot    = ev_api_poll_slot(p_api, fd);

    slot->events = 0;
    if (mask & EV_READ)
        slot->events |= POLLIN;
    if (mask & EV_WRITE)
        slot->events |= POLLOUT;
    slot->revents = 0;
    return EV_OK;
}

static int ev_api_fire_event(struct ev_ctx *ctx, int fd, int mask)
{
    struct poll_api *p_api = ctx->api;
    for (int i = 0; i < p_api->nfds; ++i) {
        if (p_api->fds[i].fd == fd) {
            p_api->fds[i].events = mask & EV_READ ? POLLIN : POLLOUT;
            break;
        }
    }
    /*
     * The array above is only read again when this loop calls poll() once
     * more: the wait it may be parked on right now was given a copy of it, so
     * it has to be woken up explicitly (see EV_BACKEND_NEEDS_WAKEUP)
     */
    ev_wakeup_notify(ctx->wakeup);
    return EV_OK;
}

/*
 * Get the event on the idx position inside the events map. The event can also
 * be an unset one (EV_NONE)
 */
static inline struct ev *ev_api_fetch_event(const struct ev_ctx *ctx, int idx,
                                            int mask)
{
    return ctx->events_monitored + ((struct poll_api *)ctx->api)->fds[idx].fd;
}

#elif defined(SELECT)

/*
 * ==========================
 *  Select backend functions
 * ==========================
 *
 * The select_api structure contains two copies of the read/write fdset, it's
 * a measure to reset the original monitored fd_set after each select(2) call
 * as it's not safe to iterate over the already selected sets, select(2) make
 * side-effects on the passed in fd_sets.
 * At each new event all the monitored fd_set are iterated and check for read
 * or write readiness; the number of monitored sockets is hard-capped at 1024.
 * It's the oldest multiplexing IO and practically obiquitous, making it the
 * perfect fallback for every system.
 */
struct select_api {
    fd_set rfds, wfds;
    // Copy of the original fdset arrays to re-initialize them after each cycle
    // we'll call them "service" fdset
    fd_set _rfds, _wfds;
};

static int ev_api_init(struct ev_ctx *ctx, int events_nr)
{
    /*
     * fd_set is an array of 32 i32 and each FD is represented by a bit so
     * 32 x 32 = 1024 as hard limit
     */
    assert(events_nr <= FD_SETSIZE);
    struct select_api *s_api = try_alloc(sizeof(*s_api));
    FD_ZERO(&s_api->rfds);
    FD_ZERO(&s_api->wfds);
    FD_ZERO(&s_api->_rfds);
    FD_ZERO(&s_api->_wfds);
    ctx->api = s_api;
    ctx->maxfd    = 0;
    return EV_OK;
}

static void ev_api_destroy(struct ev_ctx *ctx)
{
    free_memory(ctx->api);
}

static int ev_api_get_event_type(struct ev_ctx *ctx, int idx)
{
    struct select_api *s_api = ctx->api;
    int ev_mask              = ctx->events_monitored[idx].mask;
    // We want to remember the previous events only if they're not of type
    // CLOSE or TIMER
    int mask = ev_mask & (EV_CLOSEFD | EV_TIMERFD) ? ev_mask : 0;
    /*
     * Select checks all FDs by looping to the highest registered FD it
     * currently monitor. Even non set or non monitored FDs are inspected, we
     * have to ensure that the FD is currently ready for IO, otherwise we'll
     * end up looping all FDs and calling callbacks everytime, even when
     * there's no need to.
     *
     * Also we have to check for ready FDs on "service" _fdsets, cause they're
     * the ones employed on the select call to avoid side-effects on the
     * originals.
     */
    if (!FD_ISSET(idx, &s_api->_rfds) && !FD_ISSET(idx, &s_api->_wfds))
        return EV_NONE;
    if (FD_ISSET(idx, &s_api->_rfds))
        mask |= EV_READ;
    if (FD_ISSET(idx, &s_api->_wfds))
        mask |= EV_WRITE;
    return mask;
}

static int ev_api_poll(struct ev_ctx *ctx, time_t timeout)
{
    /*
     * The timeout is expressed in milliseconds, like epoll_wait(): timeval
     * splits it into seconds and microseconds, the old code stuffed the whole
     * value into tv_usec which is out of range past 999999 (1 second) and
     * made select() fail with EINVAL
     */
    struct timeval tv;
    struct timeval *tvp = NULL;
    if (timeout > 0) {
        tv.tv_sec         = timeout / 1000;
        tv.tv_usec        = (timeout % 1000) * 1000;
        tvp               = &tv;
    }
    struct select_api *s_api = ctx->api;
    // Re-initialize fdset arrays cause select call side-effect the originals
    memcpy(&s_api->_rfds, &s_api->rfds, sizeof(fd_set));
    memcpy(&s_api->_wfds, &s_api->wfds, sizeof(fd_set));
    int err = select(ctx->maxfd + 1, &s_api->_rfds, &s_api->_wfds, NULL, tvp);
    if (err < 0)
        return -EV_ERR;
    return ctx->maxfd + 1;
}

static int ev_api_watch_fd(struct ev_ctx *ctx, int fd)
{
    struct select_api *s_api = ctx->api;
    /*
     * FD_SET() on a descriptor at or beyond FD_SETSIZE writes past the end of
     * the fd_set (128 bytes on Linux), silently corrupting the stack: refuse
     * the descriptor instead
     */
    if (fd >= FD_SETSIZE) {
        errno = EINVAL;
        return -EV_ERR;
    }
    FD_SET(fd, &s_api->rfds);
    // Check for a possible new max fd, we don't want to miss events on FDs
    if (fd > ctx->maxfd)
        ctx->maxfd = fd;
    return EV_OK;
}

static int ev_api_del_fd(struct ev_ctx *ctx, int fd)
{
    struct select_api *s_api = ctx->api;
    if (fd < 0 || fd >= FD_SETSIZE)
        return EV_OK;
    if (FD_ISSET(fd, &s_api->rfds))
        FD_CLR(fd, &s_api->rfds);
    if (FD_ISSET(fd, &s_api->wfds))
        FD_CLR(fd, &s_api->wfds);
    /*
     * To remove  FD from select we must determine the new maximum descriptor
     * value based on the bits that are still turned on in the rfds set.
     * Stop at -1 (empty set): without the bound the loop walked below zero
     * and indexed the fd_set with a negative value
     */
    if (fd == ctx->maxfd) {
        while (ctx->maxfd >= 0 && !FD_ISSET(ctx->maxfd, &s_api->rfds) &&
               !FD_ISSET(ctx->maxfd, &s_api->wfds))
            ctx->maxfd -= 1;
    }
    return EV_OK;
}

static int ev_api_register_event(struct ev_ctx *ctx, int fd, int mask)
{
    struct select_api *s_api = ctx->api;
    if (fd >= FD_SETSIZE) {
        errno = EINVAL;
        return -EV_ERR;
    }
    if (mask & EV_READ)
        FD_SET(fd, &s_api->rfds);
    if (mask & EV_WRITE)
        FD_SET(fd, &s_api->wfds);
    // Check for a possible new max fd, we don't want to miss events on FDs
    if (fd > ctx->maxfd)
        ctx->maxfd = fd;
    return EV_OK;
}

static int ev_api_fire_event(struct ev_ctx *ctx, int fd, int mask)
{
    struct select_api *s_api = ctx->api;
    if (mask & EV_READ)
        FD_SET(fd, &s_api->rfds);
    if (mask & EV_WRITE)
        FD_SET(fd, &s_api->wfds);
    /* See the poll backend: the sets are copied when select() enters the
       kernel, a wait in progress has to be woken explicitly */
    ev_wakeup_notify(ctx->wakeup);
    return EV_OK;
}

/*
 * Get the event on the idx position inside the events map. The event can also
 * be an unset one (EV_NONE)
 */
static inline struct ev *ev_api_fetch_event(const struct ev_ctx *ctx, int idx,
                                            int mask)
{
    return ctx->events_monitored + idx;
}

#elif defined(KQUEUE)

/*
 * ==========================
 *  Kqueue backend functions
 * ==========================
 *
 * The Epoll counterpart on BSD systems, including Mac OSX, it's the older of
 * the two Mux IO implementations and on par in terms of performances, a bit
 * more versatile as it's possible to schedule timers directly as events
 * instead of relying on support mechanisms like `timerfd` on linux.
 */

#include <sys/event.h>
#include <sys/time.h>
#include <sys/types.h>

struct kqueue_api {
    int fd;
    struct kevent *events;
};

static int ev_api_init(struct ev_ctx *ctx, int events_nr)
{
    struct kqueue_api *k_api = try_alloc(sizeof(*k_api));
    k_api->fd                = kqueue();
    k_api->events            = try_calloc(events_nr, sizeof(struct kevent));
    ctx->api                 = k_api;
    ctx->maxfd               = events_nr;
    return EV_OK;
}

static void ev_api_destroy(struct ev_ctx *ctx)
{
    close(((struct kqueue_api *)ctx->api)->fd);
    free_memory(((struct kqueue_api *)ctx->api)->events);
    free_memory(ctx->api);
}

static int ev_api_get_event_type(struct ev_ctx *ctx, int idx)
{
    struct kqueue_api *k_api = ctx->api;
    /*
     * The filter that fired lives in .filter, while .flags carries EV_EOF,
     * EV_ERROR and friends. The old code tested `flags & EVFILT_READ`, but
     * EVFILT_READ is -1, so any non-zero flags read as "readable" and a zero
     * value suppressed both directions: the dispatch was effectively random
     */
    int16_t filter           = k_api->events[idx].filter;
    uint16_t keflags         = k_api->events[idx].flags;
    int ev_mask = ctx->events_monitored[k_api->events[idx].ident].mask;
    // We want to remember the previous events only if they're not of type
    // CLOSE or TIMER
    int mask    = ev_mask & (EV_CLOSEFD | EV_TIMERFD) ? ev_mask : EV_NONE;
    if (keflags & (EV_EOF | EV_ERROR))
        mask |= EV_DISCONNECT;
    if (filter == EVFILT_READ)
        mask |= EV_READ;
    if (filter == EVFILT_WRITE)
        mask |= EV_WRITE;
    return mask;
}

static int ev_api_poll(struct ev_ctx *ctx, time_t timeout)
{
    struct kqueue_api *k_api = ctx->api;
    struct timespec ts_timeout;
    struct timespec *tsp = NULL;
    /*
     * ev_run() passes -1 for "block forever", which produced a negative
     * timespec and made kevent() fail with EINVAL, exiting the loop right
     * after the first cycle on BSD/macOS. A NULL timeout is the documented
     * way to wait indefinitely
     */
    if (timeout >= 0) {
        ts_timeout.tv_sec  = timeout;
        ts_timeout.tv_nsec = 0;
        tsp                = &ts_timeout;
    }
    int err = kevent(k_api->fd, NULL, 0, k_api->events, ctx->events_nr, tsp);
    if (err < 0)
        return -EV_ERR;
    return err;
}

static int ev_api_del_fd(struct ev_ctx *ctx, int fd)
{
    struct kqueue_api *k_api = ctx->api;
    int ev_mask              = ctx->events_monitored[fd].mask;
    /*
     * A kevent is per filter: EVFILT_READ, EVFILT_WRITE and EVFILT_TIMER are
     * distinct (negative) constants and must never be OR-ed together, the
     * result of the old `mask |=` was a filter value no registration ever
     * used, so the delete silently failed and left the filter armed
     */
    struct kevent ke[3];
    int n = 0;
    if (ev_mask & EV_READ)
        EV_SET(&ke[n++], fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
    if (ev_mask & EV_WRITE)
        EV_SET(&ke[n++], fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
    if (ev_mask & EV_TIMERFD)
        EV_SET(&ke[n++], fd, EVFILT_TIMER, EV_DELETE, 0, 0, NULL);
    if (n == 0)
        return EV_OK;
    if (kevent(k_api->fd, ke, n, NULL, 0, NULL) == -1)
        return -EV_ERR;
    return EV_OK;
}

static int ev_api_register_event(struct ev_ctx *ctx, int fd, int mask)
{
    struct kqueue_api *k_api = ctx->api;
    /*
     * One kevent per filter, see ev_api_del_fd(): OR-ing the filter
     * constants together registered only EVFILT_READ (EVFILT_READ is -1, so
     * `-1 | EVFILT_WRITE` is still -1) and a descriptor watched for writing
     * never became writable
     */
    struct kevent ke[2];
    int n = 0;
    if (mask & EV_READ)
        EV_SET(&ke[n++], fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, NULL);
    if (mask & EV_WRITE)
        EV_SET(&ke[n++], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, NULL);
    if (n == 0)
        return EV_OK;
    if (kevent(k_api->fd, ke, n, NULL, 0, NULL) == -1)
        return -EV_ERR;
    return EV_OK;
}

static int ev_api_watch_fd(struct ev_ctx *ctx, int fd)
{
    return ev_api_register_event(ctx, fd, EV_READ);
}

static int ev_api_fire_event(struct ev_ctx *ctx, int fd, int mask)
{
    struct kqueue_api *k_api = ctx->api;
    /* One kevent per filter, same reasoning as ev_api_register_event() */
    struct kevent ke[2];
    int n = 0;
    if (mask & (EV_READ | EV_EVENTFD))
        EV_SET(&ke[n++], fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, NULL);
    if (mask & EV_WRITE)
        EV_SET(&ke[n++], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, NULL);
    if (n == 0)
        return EV_OK;
    if (kevent(k_api->fd, ke, n, NULL, 0, NULL) == -1)
        return -EV_ERR;
    return EV_OK;
}

/*
 * Get the event on the idx position inside the events map. The event can also
 * be an unset one (EV_NONE)
 */
static inline struct ev *ev_api_fetch_event(const struct ev_ctx *ctx, int idx,
                                            int mask)
{
    (void)mask; // silence compiler warning
    int fd = ((struct kqueue_api *)ctx->api)->events[idx].ident;
    return ctx->events_monitored + fd;
}

#endif // KQUEUE

/*
 * Process the event at the position idx in the events_monitored array. Read or
 * write events can be executed on the same iteration, differentiating just
 * on EV_CLOSEFD or EV_EVENTFD.
 * Returns the number of fired callbacks.
 */
static int ev_process_event(struct ev_ctx *ctx, int idx, int mask)
{
    if (mask == EV_NONE)
        return EV_OK;

    /*
     * The slot is copied out under the lock: another loop may be rewriting it
     * right now (it fires an event at a client living here), and the
     * callbacks themselves are invoked with the lock released, they call back
     * into this module (a rearm, another fire) and would deadlock
     */
    pthread_mutex_lock(&ctx->lock);
    struct ev *e = ev_api_fetch_event(ctx, idx, mask);
    struct ev snapshot = *e;
    pthread_mutex_unlock(&ctx->lock);
    e = &snapshot;

    int err = 0, fired = 0, fd = e->fd;
    if (mask & EV_CLOSEFD) {
        /*
         * The descriptor (the shared run eventfd) is watched by every event
         * loop thread: it must NOT be read here, because reading resets the
         * counter for everyone and the loops that lose the race would get an
         * EAGAIN below and never fire their stop callback, hanging the
         * shutdown on pthread_join. Leaving the counter untouched keeps the
         * fd level-triggered ready until every loop has observed it; the
         * descriptor is released together with the process anyway
         */
        /*
         * The shared run eventfd is never removed from a loop while it is
         * being processed, but guard it the same way the read branch below
         * does: a slot zeroed by a concurrent ev_del_fd would otherwise be
         * dispatched through a NULL callback
         */
        if (e->rcallback) {
            e->rcallback(ctx, e->rdata);
            ++fired;
        }
    } else {
        if (mask & EV_EVENTFD) {
#ifdef __linux__
            err = eventfd_read(fd, &(eventfd_t){0L});
#else
            err = read(fd, &(unsigned long){0}, sizeof(unsigned long));
#endif // __linux__
            close(fd);
        } else if (mask & EV_TIMERFD) {
            err = read(fd, &(unsigned long int){0L}, sizeof(unsigned long int));
        }
        if (err < 0)
            return EV_OK;
        /*
         * EPOLLERR/EPOLLHUP can arrive with no EPOLLIN interest armed (the
         * descriptor is only watched for writing and the peer reset the
         * connection), leaving mask as EV_DISCONNECT alone. Nothing below
         * would run, and since epoll is level triggered it would report the
         * very same descriptor at every cycle: a 100% CPU spin that also
         * never frees the connection. Hand it to the read callback, which
         * observes the error and runs the regular teardown; without a read
         * callback there is nothing to salvage, so the write one gets it
         */
        if (mask & EV_DISCONNECT && !(mask & (EV_READ | EV_WRITE))) {
            if (e->rcallback) {
                e->rcallback(ctx, e->rdata);
                ++fired;
            } else if (e->wcallback) {
                e->wcallback(ctx, e->wdata);
                ++fired;
            }
            return fired;
        }
        if (mask & EV_READ && e->rcallback) {
            /*
             * A descriptor removed from the loop while this batch of events
             * was already fired leaves a zeroed slot behind: skip it
             * instead of calling through a NULL pointer
             */
            e->rcallback(ctx, e->rdata);
            ++fired;
        }
        if (mask & EV_WRITE && e->wcallback) {
            if (!fired || e->wcallback != e->rcallback) {
                e->wcallback(ctx, e->wdata);
                ++fired;
            }
        }
    }
    return fired;
}

/*
 * Auxiliary function, update FD, mask and data in monitored events array.
 * Monitored events are the same number as the maximum FD registered in the
 * context.
 */
static void ev_add_monitored(struct ev_ctx *ctx, int fd, int mask,
                             void (*callback)(struct ev_ctx *, void *),
                             void *ptr)
{
    pthread_mutex_lock(&ctx->lock);
    /*
     * A negative descriptor is a connection that has been closed, and its
     * events may still be in flight: the slot it used to own is zeroed by
     * ev_del_fd(), and indexing the array with -1 would write right before
     * its first element
     */
    if (fd < 0)
        return;

    /*
     * Grow the monitored array when the descriptor does not fit. The check
     * has to be `>=`: the array holds monitored_nr slots, indexed 0 ..
     * monitored_nr - 1, so an fd *equal* to the capacity is already past its
     * end. The old `>` comparison wrote a whole struct ev beyond the block
     * as soon as a descriptor reached the initial size (1024), corrupting
     * the heap on any machine allowing that many open files.
     *
     * Every slot of the grown range is initialized, the one at `fd`
     * included: realloc does not zero the new memory and the `mask |=` below
     * would OR our bit with whatever garbage is there. A leftover EV_EVENTFD
     * bit makes the dispatch close() a perfectly live client descriptor, a
     * leftover EV_TIMERFD bit read()s bytes off it and desynchronizes the
     * whole MQTT stream, a leftover EV_CLOSEFD stops the loop.
     */
    if (fd >= ctx->monitored_nr) {
        int old_nr   = ctx->monitored_nr;
        int new_nr   = fd + 1;
        ctx->events_monitored = try_realloc(ctx->events_monitored,
                                            new_nr * sizeof(struct ev));
        for (int i = old_nr; i < new_nr; ++i)
            ctx->events_monitored[i] = (struct ev){.fd = -1, .mask = EV_NONE};
        ctx->monitored_nr = new_nr;
    }

    /* Track the highest descriptor registered, for the backends that need it */
    if (fd > ctx->maxevents)
        ctx->maxevents = fd;

    ctx->events_monitored[fd].fd = fd;
    ctx->events_monitored[fd].mask |= mask;
    if (mask & EV_READ) {
        ctx->events_monitored[fd].rdata     = ptr;
        ctx->events_monitored[fd].rcallback = callback;
    }
    if (mask & EV_WRITE) {
        ctx->events_monitored[fd].wdata     = ptr;
        ctx->events_monitored[fd].wcallback = callback;
    }
    pthread_mutex_unlock(&ctx->lock);
}

static inline int ev_get_event_type(struct ev_ctx *ctx, int idx)
{
    return ev_api_get_event_type(ctx, idx);
}

int ev_init(struct ev_ctx *ctx, int events_nr)
{
    int err = ev_api_init(ctx, events_nr);
    if (err < 0)
        return err;
    pthread_mutex_init(&ctx->lock, NULL);
    ctx->stop             = 0;
    ctx->fired_events     = 0;
    ctx->maxevents        = events_nr;
    ctx->events_nr        = events_nr;
    ctx->monitored_nr     = events_nr;
    ctx->events_monitored = try_calloc(events_nr, sizeof(struct ev));

#if EV_BACKEND_NEEDS_WAKEUP
    /*
     * One wakeup descriptor per loop, watched like any other read descriptor:
     * this is what lets another thread pull this loop out of a blocking wait
     * when it rearms one of its descriptors (see ev_wakeup_notify). It is
     * registered here, once the monitored array exists
     */
    ctx->wakeup = ev_wakeup_create();
    if (ctx->wakeup >= 0) {
        ev_add_monitored(ctx, ctx->wakeup, EV_READ, ev_wakeup_callback, NULL);
        ev_api_watch_fd(ctx, ctx->wakeup);
    }
#else
    ctx->wakeup = -1;
#endif

    return EV_OK;
}

void ev_destroy(struct ev_ctx *ctx)
{
    /* No other loop can reach this context anymore: the caller joins every
       thread before destroying them (see start_server) */
    /*
     * Walk the whole capacity of the array: maxevents only tracks the highest
     * descriptor ever registered and would leave the last slot unvisited
     */
    for (int i = 0; i < ctx->monitored_nr; ++i) {
        if (!(ctx->events_monitored[i].mask & EV_CLOSEFD) &&
            ctx->events_monitored[i].mask != EV_NONE)
            ev_del_fd(ctx, ctx->events_monitored[i].fd);
    }
    if (ctx->wakeup >= 0)
        close(ctx->wakeup);
    pthread_mutex_destroy(&ctx->lock);
    free_memory(ctx->events_monitored);
    ctx->events_monitored = NULL;
    ctx->monitored_nr     = 0;
    ev_api_destroy(ctx);
}

int ev_poll(struct ev_ctx *ctx, time_t timeout)
{
    return ev_api_poll(ctx, timeout);
}

int ev_run(struct ev_ctx *ctx)
{
    int n = 0, events = 0;
    /*
     * Start an infinite loop, can be stopped only by scheduling an ev_stop
     * callback or if an error on the underlying backend occur
     */
    while (!ctx->stop) {
        /*
         * blocks polling for events, -1 means forever. Returns only in case of
         * valid events ready to be processed or errors
         */
        n = ev_poll(ctx, -1);
        if (n < 0) {
            /* Signals to all threads. Ignore it for now */
            if (errno == EINTR)
                continue;
            /* Error occured, break the loop */
            break;
        }
        for (int i = 0; i < n; ++i) {
            pthread_mutex_lock(&ctx->lock);
            events = ev_get_event_type(ctx, i);
            pthread_mutex_unlock(&ctx->lock);
            ctx->fired_events += ev_process_event(ctx, i, events);
        }
    }
    return n;
}

void ev_stop(struct ev_ctx *ctx) { ctx->stop = 1; }

int ev_watch_fd(struct ev_ctx *ctx, int fd, int mask)
{
    /* A closed descriptor has nothing to monitor (see ev_add_monitored) */
    if (fd < 0)
        return EV_OK;
    ev_add_monitored(ctx, fd, mask, NULL, NULL);
    return ev_api_watch_fd(ctx, fd);
}

int ev_del_fd(struct ev_ctx *ctx, int fd)
{
    /* A descriptor beyond the array was never registered: nothing to clear */
    if (fd < 0 || fd >= ctx->monitored_nr)
        return ev_api_del_fd(ctx, fd);

    pthread_mutex_lock(&ctx->lock);
    memset(ctx->events_monitored + fd, 0x00, sizeof(struct ev));
    ctx->events_monitored[fd].fd = -1;
    pthread_mutex_unlock(&ctx->lock);
    return ev_api_del_fd(ctx, fd);
}

/*
 * Set a callback and an argument to be passed to for the next loop cycle,
 * associating it to a file descriptor, ultimately resulting in an event to be
 * dispatched and processed.
 *
 * The difference with ev_fire_event is that this function should be called
 * when the file descriptor is not registered in the loop yet.
 *
 * - mask: bitmask used to describe what type of event we're going to fire
 * - callback:  is a function pointer to the routine we want to execute
 * - data:  an opaque pointer to the arguments for the callback.
 */
int ev_register_event(struct ev_ctx *ctx, int fd, int mask,
                      void (*callback)(struct ev_ctx *, void *), void *data)
{
    if (fd < 0)
        return -EV_ERR;
    ev_add_monitored(ctx, fd, mask, callback, data);
    int ret = 0;
    ret     = ev_api_register_event(ctx, fd, mask);
    if (ret < 0)
        return -EV_ERR;
    if (mask & EV_EVENTFD)
#ifdef __linux__
        (void)eventfd_write(fd, 1);
#else
        (void)write(fd, &(unsigned long){1}, sizeof(unsigned long));
#endif
    return EV_OK;
}

/*
 * Register a periodically repeate callback and args to be passed to a running
 * loop, specifying, seconds and/or nanoseconds defining how often the callback
 * should be executed.
 */
int ev_register_cron(struct ev_ctx *ctx,
                     void (*callback)(struct ev_ctx *, void *), void *data,
                     long long s, long long ns)
{
#ifdef __linux__
    struct itimerspec timer;
    memset(&timer, 0x00, sizeof(timer));
    timer.it_value.tv_sec     = s;
    timer.it_value.tv_nsec    = ns;
    timer.it_interval.tv_sec  = s;
    timer.it_interval.tv_nsec = ns;

    int timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timerfd < 0) {
        perror("timerfd_create");
        return -EV_ERR;
    }

    if (timerfd_settime(timerfd, 0, &timer, NULL) < 0) {
        /* Release the descriptor or every failed attempt leaks one */
        perror("timerfd_settime");
        (void)close(timerfd);
        return -EV_ERR;
    }

    // Add the timer to the event loop
    ev_add_monitored(ctx, timerfd, EV_TIMERFD | EV_READ, callback, data);
    return ev_api_watch_fd(ctx, timerfd);
#else
    struct kqueue_api *k_api = ctx->api;
    /*
     * EVFILT_TIMER counts in milliseconds: the old conversion divided the
     * nanoseconds by 100 (giving tenths of a millisecond) so every period was
     * 10000x shorter than requested
     */
    unsigned period          = (s * 1000) + (ns / 1000000);
    /*
     * The timer is identified by its own descriptor, created for the purpose
     * and closed when the loop is destroyed (the old code reused a socket()
     * descriptor, never closed, which could collide with an accepted client
     * fd and made kevent watch the wrong object)
     */
    int fd                   = kqueue();
    if (fd < 0) {
        perror("kqueue");
        return -EV_ERR;
    }
    ev_add_monitored(ctx, fd, EV_TIMERFD | EV_READ, callback, data);
    struct kevent ke;
    EV_SET(&ke, fd, EVFILT_TIMER, EV_ADD | EV_ENABLE, 0, period, 0);
    if (kevent(k_api->fd, &ke, 1, NULL, 0, NULL) == -1) {
        perror("kevent");
        (void)close(fd);
        return -EV_ERR;
    }
    return EV_OK;
#endif /* __linux__ */
}

/*
 * Set a callback and an argument to be passed to for the next loop cycle,
 * associating it to a file descriptor, ultimately resulting in an event to be
 * dispatched and processed.
 *
 * Behave like ev_register_event but it's meant to be called when the file
 * descriptor is already registered in the loop.
 *
 * - mask: bitmask used to describe what type of event we're going to fire
 * - callback:  is a function pointer to the routine we want to execute
 * - data:  an opaque pointer to the arguments for the callback.
 */
int ev_fire_event(struct ev_ctx *ctx, int fd, int mask,
                  void (*callback)(struct ev_ctx *, void *), void *data)
{
    int ret = 0;
    /*
     * An event fired at a descriptor that has been closed in the meantime (a
     * connection deactivated while its write was still queued) has nothing to
     * deliver: the backends would be handed a -1, and the select one shifts
     * it into the descriptor set, corrupting memory next to it
     */
    if (fd < 0)
        return EV_OK;
    ev_add_monitored(ctx, fd, mask, callback, data);
    /*
     * The poll and select backends rearm by writing the descriptor set their
     * loop is waiting on, and this call comes from another loop: the context
     * lock serializes it with the reads of the owner (epoll talks to the
     * kernel instead and does not need it, but the lock is taken the same
     * way, it only costs an uncontended futex)
     */
    pthread_mutex_lock(&ctx->lock);
    ret = ev_api_fire_event(ctx, fd, mask);
    pthread_mutex_unlock(&ctx->lock);
    if (ret < 0)
        return -EV_ERR;
    if (mask & EV_EVENTFD) {
#ifdef __linux__
        ret = eventfd_write(fd, 1);
#else
        ret = write(fd, &(unsigned long){1}, sizeof(unsigned long));
#endif // __linux__
        if (ret < 0)
            return -EV_ERR;
    }
    return EV_OK;
}
