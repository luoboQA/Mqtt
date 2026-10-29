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

#ifndef CONFIG_H
#define CONFIG_H

#include "uthash.h"
#include <stdbool.h>

// Eventloop backend check
//
// The backend can be forced at compile time (-DEVENTLOOP=poll on the cmake
// command line, or -DSOL_EVENTLOOP_* by hand), which is what makes the three
// implementations testable on a single machine: the kernel version only
// decides when nothing is forced, and on anything but Linux the override is
// ignored (kqueue is the only backend there)
#ifdef __linux__
#include <linux/version.h>
#if defined(SOL_EVENTLOOP_EPOLL)
#define EPOLL             1
#define EVENTLOOP_BACKEND "epoll"
#elif defined(SOL_EVENTLOOP_POLL)
#define POLL              1
#define EVENTLOOP_BACKEND "poll"
#elif defined(SOL_EVENTLOOP_SELECT)
#define SELECT            1
#define EVENTLOOP_BACKEND "select"
#elif LINUX_VERSION_CODE >= KERNEL_VERSION(2, 5, 44)
#define EPOLL             1
#define EVENTLOOP_BACKEND "epoll"
#elif LINUX_VERSION_CODE >= KERNEL_VERSION(2, 1, 23)
#define POLL              1
#define EVENTLOOP_BACKEND "poll"
#else
#define SELECT            1
#define EVENTLOOP_BACKEND "select"
#endif

#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) ||    \
    defined(__NetBSD__)
#define KQUEUE            1
#define EVENTLOOP_BACKEND "kqueue"
#else
#define SELECT            1
#define EVENTLOOP_BACKEND "select"
#endif // __linux__

// TLS versions

#define SOL_TLSv1                0x01
#define SOL_TLSv1_1              0x02
#define SOL_TLSv1_2              0x04
#define SOL_TLSv1_3              0x08

// Default parameters

#define VERSION                  "0.18.5"
#define DEFAULT_SOCKET_FAMILY    INET
#define DEFAULT_LOG_LEVEL        DEBUG
#define DEFAULT_CONF_PATH        "etc/conf/sol.conf"
#define DEFAULT_HOSTNAME         "127.0.0.1"
#define DEFAULT_PORT             "1883"
#define DEFAULT_MAX_MEMORY       "2GB"
#define DEFAULT_MAX_REQUEST_SIZE "512KB"
#define DEFAULT_STATS_INTERVAL   "10s"
#define DEFAULT_KEEPALIVE        "60s"
#define DEFAULT_PERSISTENCE_INTERVAL "30s"
#define DEFAULT_RECLAIM_INTERVAL "60s"
#define DEFAULT_RETRANSMIT       "20s"
#define DEFAULT_MAX_INFLIGHT     20
#ifdef TLS1_3_VERSION
#define DEFAULT_TLS_PROTOCOLS (SOL_TLSv1_2 | SOL_TLSv1_3)
#else
#define DEFAULT_TLS_PROTOCOLS SOL_TLSv1_2
#endif

struct config {
    /* Sol version <MAJOR.MINOR.PATCH> */
    const char *version;
    /* Eventfd to break the epoll_wait loop in case of signals */
#ifdef __linux__
    int run;
#else
    int run[2];
#endif // __linux__
    /* Logging level, to be set by reading configuration */
    int loglevel;
    /* Socket family (Unix domain or TCP) */
    int socket_family;
    /* Log file path */
    char logpath[0xFFF];
    /* Hostname to listen on */
    char hostname[0xFF];
    /* Port to open while listening, only if socket_family is INET,
     * otherwise it's ignored */
    char port[0xFF];
    /* Max memory to be used, after which the system starts to reclaim back by
     * freeing older items stored */
    size_t max_memory;
    /* Max memory request can allocate */
    size_t max_request_size;
    /* TCP backlog size */
    int tcp_backlog;
    /* Delay between every automatic publish of broker stats on topic */
    size_t stats_pub_interval;
    /*
     * Seconds to keep alive any connection,
     * **CURRENTLY USED AS ACK TIMER AS WELL**
     */
    size_t keepalive;
    /*
     * Seconds after which an unacknowledged QoS 1/2 message (or a PUBREL
     * still waiting for its PUBCOMP) is re-transmitted with the DUP flag
     */
    size_t retransmit_interval;
    /* TLS flag */
    bool tls;
    /* TLS protocol version */
    int tls_protocols;
    /* Certificate authority file path */
    char cafile[0xFFF];
    /* SSL - Cert file location on filesystem */
    char certfile[0xFFF];
    /* SSL - Key file location on filesystem */
    char keyfile[0xFFF];
    /* Authentication flag */
    bool allow_anonymous;
    /* File path on the filesystem pointing to the password_file */
    char password_file[0xFFF];
    /*
     * File holding the topic level authorization rules. Empty (the default)
     * leaves the broker without an access control list: every authenticated
     * client may read and write every topic
     */
    char acl_file[0xFFF];
    /*
     * File the broker snapshots its stored state to (the retained messages
     * and the persistent sessions with their queued messages). An empty path
     * disables persistence entirely, which is the default
     */
    char persistence_file[0xFFF];
    /* Seconds between two periodic snapshots, only written when the state
     * changed since the previous one */
    size_t persistence_interval;
    /*
     * Seconds between two reclamations of the topics nothing refers to
     * anymore, while the broker is *not* over its max_memory (under pressure
     * the reclamation runs on every memory check instead)
     */
    size_t reclaim_interval;
    /*
     * File the daemon writes its PID to once it has detached, and removes on
     * a clean shutdown. Empty (the default) writes no PID file. Only
     * meaningful with -d, but accepted without it for service managers that
     * always pass the same configuration
     */
    char pid_file[0xFFF];
    /*
     * Maximum number of QoS 1/2 messages a session may have in flight (sent
     * and awaiting an acknowledgement, or queued while the client is away).
     * Once the window is full new messages for that session are dropped, the
     * way a server resource limit is allowed to behave, instead of letting a
     * single slow subscriber grow the queue without bound. 0 disables the
     * cap and keeps the old behaviour
     */
    unsigned max_inflight;
    /*
     * Maximum number of PUBLISH packets per second accepted from one client,
     * as a token bucket refilled every second. Over the rate the packet is
     * dropped, but the QoS 1/2 acknowledgement still flows so the client is
     * not left retransmitting it. 0 (the default) disables the limit
     */
    unsigned max_publish_rate;
    /*
     * Maximum number of simultaneous connections from one peer address. Over
     * the limit the new connection is closed right after accept. 0 (the
     * default) disables the limit
     */
    unsigned max_connections_per_ip;
    /*
     * Ask every TLS client for a certificate signed by `cafile` and refuse
     * the handshake when it does not present a valid one (mutual TLS)
     */
    bool require_client_cert;
};

extern struct config *conf;

/*
 * Authentication structure to represent a login composed by username and salt.
 * It's an hashable structure, for more info check
 * https://troydhanson.github.io/uthash/userguide.html
 */
struct authentication {
    char *username;
    char *salt;
    UT_hash_handle hh; /* UTHASH handle, needed to use UTHASH macros */
};

#define AUTH_DESTROY(auth_map)                                                 \
    do {                                                                       \
        struct authentication *auth, *dummy;                                   \
        HASH_ITER(hh, (auth_map), auth, dummy)                                 \
        {                                                                      \
            HASH_DEL((auth_map), auth);                                        \
            free_memory(auth->username);                                       \
            free_memory(auth->salt);                                           \
            free_memory(auth);                                                 \
        }                                                                      \
    } while (0);

void config_set_default(void);
void config_print(void);
int config_load(const char *);
/*
 * Cross field and file checks (port range, TLS files, password/ACL files).
 * config_load() only parses; this is what --check-config and the startup
 * path call once the command line overrides are applied.
 */
bool config_check(void);
bool config_read_passwd_file(const char *, struct authentication **);
char *time_to_string(size_t);
char *memory_to_string(size_t);

#endif
