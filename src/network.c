/*
 * BSD 2-Clause License
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

#include "network.h"
#include "config.h"
#include "memory.h"
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <openssl/err.h>
#include <stdlib.h>
#include <sys/un.h>
#include <unistd.h>

/* Set non-blocking socket */
static int set_nonblocking(int fd)
{
    int flags, result;
    flags = fcntl(fd, F_GETFL, 0);

    if (flags == -1)
        goto err;

    result = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    if (result == -1)
        goto err;

    return 0;

err:

    perror("set_nonblocking");
    return -1;
}

static int set_cloexec(int fd)
{
    int flags, result;

    /*
     * F_GETFD/F_SETFD operate on the file descriptor flags, where FD_CLOEXEC
     * lives. The old code read the file status flags (F_GETFL) and wrote
     * them into the descriptor flags: it happened to work because FD_CLOEXEC
     * is bit 0, but it also leaked O_* access-mode bits into the wrong word
     */
    flags = fcntl(fd, F_GETFD, 0);

    if (flags == -1)
        goto err;

    result = fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
    if (result == -1)
        goto err;

    return 0;

err:

    perror("set_cloexec");
    return -1;
}

/*
 * Set TCP_NODELAY flag to true, disabling Nagle's algorithm, no more waiting
 * for incoming packets on the buffer
 */
static int set_tcpnodelay(int fd)
{
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &(int){1}, sizeof(int));
}

static int create_and_bind_unix(const char *sockpath)
{

    struct sockaddr_un addr;
    int fd;

    if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        perror("socket error");
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;

    strncpy(addr.sun_path, sockpath, sizeof(addr.sun_path) - 1);
    unlink(sockpath);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        perror("bind error");
        return -1;
    }

    return fd;
}

static int create_and_bind_tcp(const char *host, const char *port)
{

    struct addrinfo hints = {.ai_family   = AF_UNSPEC,
                             .ai_socktype = SOCK_STREAM,
                             .ai_flags    = AI_PASSIVE};

    struct addrinfo *result, *rp;
    int sfd;

    if (getaddrinfo(host, port, &hints, &result) != 0)
        goto err;

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        sfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);

        if (sfd == -1)
            continue;

        /* set SO_REUSEADDR so the socket will be reusable after process kill */
        if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) <
            0)
            perror("SO_REUSEADDR");

        if ((bind(sfd, rp->ai_addr, rp->ai_addrlen)) == 0) {
            /* Succesful bind */
            break;
        }
        (void)close(sfd);
    }

    freeaddrinfo(result);

    if (rp == NULL)
        goto err;

    return sfd;

err:

    perror("Unable to bind socket");
    return -1;
}

/* Auxiliary function for binding a socket to listen on defined port */
static int create_and_bind(const char *host, const char *port, int s_family)
{
    return s_family == UNIX ? create_and_bind_unix(host)
                            : create_and_bind_tcp(host, port);
}

/*
 * Create a non-blocking socket and make it listen on the specfied address and
 * port
 */
int make_listen(const char *host, const char *port, int s_family)
{

    int sfd;

    /*
     * A bind/listen failure at startup is not a crash: refuse to start with
     * a clear message and a normal exit code instead of abort(), which would
     * look like a SIGABRT crash (134) whenever a previous instance is still
     * shutting down and still holds the port
     */
    if ((sfd = create_and_bind(host, port, s_family)) == -1) {
        fprintf(stderr,
                "Unable to listen on %s:%s, is another broker instance "
                "already running on that address?\n",
                host, port);
        exit(EXIT_FAILURE);
    }

    if ((set_nonblocking(sfd)) == -1)
        exit(EXIT_FAILURE);

    if ((set_cloexec(sfd)) == -1)
        exit(EXIT_FAILURE);

    // Set TCP_NODELAY only for TCP sockets
    if (s_family == INET)
        (void)set_tcpnodelay(sfd);

    if ((listen(sfd, conf->tcp_backlog)) == -1) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    return sfd;
}

/*
 * Accept a connection and set it NON_BLOCKING and CLOEXEC, optionally also set
 * TCP_NODELAY disabling Nagle's algorithm
 */
static int accept_conn(int sfd, char *ip)
{

    int clientsock;
    /*
     * The listening socket is created with AF_UNSPEC, so it can be IPv4 or
     * IPv6: a struct sockaddr_in would truncate an sockaddr_in6 and report a
     * wrong address (and port) in the logs, sockaddr_storage covers both
     */
    struct sockaddr_storage addr;
    socklen_t addrlen = sizeof(addr);

    if ((clientsock = accept(sfd, (struct sockaddr *)&addr, &addrlen)) < 0) {
        /* Retry on a signal, report anything else */
        if (errno == EINTR)
            return accept_conn(sfd, ip);
        if (errno != EWOULDBLOCK && errno != EAGAIN)
            perror("accept");
        return -1;
    }

    (void)set_nonblocking(clientsock);
    (void)set_cloexec(clientsock);

    // Set TCP_NODELAY only for TCP sockets
    if (conf->socket_family == INET)
        (void)set_tcpnodelay(clientsock);

    char ip_buff[INET6_ADDRSTRLEN];
    if (addr.ss_family == AF_INET6) {
        struct sockaddr_in6 *in6 = (struct sockaddr_in6 *)&addr;
        if (inet_ntop(AF_INET6, &in6->sin6_addr, ip_buff, sizeof(ip_buff)) ==
            NULL) {
            if (close(clientsock) < 0)
                perror("close");
            return -1;
        }
        if (ip)
            snprintf(ip, INET6_ADDRSTRLEN + 8, "[%s]:%i", ip_buff,
                     ntohs(in6->sin6_port));
    } else {
        struct sockaddr_in *in4 = (struct sockaddr_in *)&addr;
        if (inet_ntop(AF_INET, &in4->sin_addr, ip_buff, sizeof(ip_buff)) ==
            NULL) {
            if (close(clientsock) < 0)
                perror("close");
            return -1;
        }
        if (ip)
            snprintf(ip, INET6_ADDRSTRLEN + 8, "%s:%i", ip_buff,
                     ntohs(in4->sin_port));
    }

    return clientsock;
}

/* Send all bytes contained in buf, updating sent bytes counter */
ssize_t send_bytes(int fd, const unsigned char *buf, size_t len)
{

    size_t total     = 0;
    size_t bytesleft = len;
    ssize_t n        = 0;

    while (total < len) {
        n = send(fd, buf + total, bytesleft, MSG_NOSIGNAL);
        if (n == -1) {
            /* A signal must not be mistaken for a socket failure: retry */
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            else
                goto err;
        }
        total += n;
        bytesleft -= n;
    }

    return total;

err:

    fprintf(stderr, "send(2) - error sending data: %s\n", strerror(errno));
    return -1;
}

/*
 * Receive a given number of bytes on the descriptor fd, storing the stream of
 * data into a 2 Mb capped buffer
 */
ssize_t recv_bytes(int fd, unsigned char *buf, size_t bufsize)
{

    ssize_t n     = 0;
    ssize_t total = 0;

    while (total < (ssize_t)bufsize) {

        if ((n = recv(fd, buf, bufsize - total, 0)) < 0) {
            /* A signal must not be mistaken for a socket failure: retry */
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            else
                goto err;
        }

        if (n == 0)
            return 0;

        buf += n;
        total += n;
    }

    return total;

err:

    fprintf(stderr, "recv(2) - error reading data: %s\n", strerror(errno));
    return -1;
}

void openssl_init(void)
{
    SSL_library_init();
    ERR_load_crypto_strings();
    SSL_load_error_strings();
    OpenSSL_add_ssl_algorithms();
}

void openssl_cleanup(void) { EVP_cleanup(); }

SSL_CTX *create_ssl_context(void)
{

    SSL_CTX *ctx;

#if OPENSSL_VERSION_NUMBER >= 0x10100000
    // TLS_server_method has been added with OpenSSL version > 1.1.0
    // and should be used in place of SSLv* which is goind to be deprecated
    ctx = SSL_CTX_new(TLS_server_method());
#else
    ctx = SSL_CTX_new(SSLv23_method());
#endif /* OPENSSL_VERSION_NUMBER */

    if (!ctx) {
        perror("Unable to create SSL context");
        ERR_print_errors_fp(stderr);
        exit(EXIT_FAILURE);
    }

    SSL_CTX_set_options(ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);
    SSL_CTX_set_options(ctx, SSL_OP_SINGLE_DH_USE);

    if (!(conf->tls_protocols & SOL_TLSv1))
        SSL_CTX_set_options(ctx, SSL_OP_NO_TLSv1);
    if (!(conf->tls_protocols & SOL_TLSv1_1))
        SSL_CTX_set_options(ctx, SSL_OP_NO_TLSv1_1);
#ifdef SSL_OP_NO_TLSv1_2
    if (!(conf->tls_protocols & SOL_TLSv1_2))
        SSL_CTX_set_options(ctx, SSL_OP_NO_TLSv1_2);
#endif
#ifdef SSL_OP_NO_TLSv1_3
    if (!(conf->tls_protocols & SOL_TLSv1_3))
        SSL_CTX_set_options(ctx, SSL_OP_NO_TLSv1_3);
#endif

#ifdef SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS
    SSL_CTX_set_options(ctx, SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS);
#endif
#ifdef SSL_OP_NO_COMPRESSION
    SSL_CTX_set_options(ctx, SSL_OP_NO_COMPRESSION);
#endif
#ifdef SSL_OP_NO_CLIENT_RENEGOTIATION
    SSL_CTX_set_options(ctx, SSL_OP_NO_CLIENT_RENEGOTIATION);
#endif

    return ctx;
}

static int client_certificate_verify(int preverify_ok, X509_STORE_CTX *ctx)
{

    (void)ctx; // Unused

    /* Preverify should check expiry, revocation. */
    return preverify_ok;
}

void load_certificates(SSL_CTX *ctx, const char *ca, const char *cert,
                       const char *key, bool require_client_cert)
{

    if (SSL_CTX_load_verify_locations(ctx, ca, NULL) <= 0) {
        ERR_print_errors_fp(stderr);
        exit(EXIT_FAILURE);
    }

    SSL_CTX_set_mode(ctx, SSL_MODE_ENABLE_PARTIAL_WRITE |
                              SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
    /*
     * Without require_client_cert the broker authenticates itself to the
     * client and asks for nothing in return (the default, what a plain TLS
     * listener does). With it the handshake only completes when the client
     * presents a certificate that chains to `ca`: SSL_VERIFY_FAIL_IF_NO_PEER_CERT
     * is what turns "ask for one" into "require one", SSL_VERIFY_PEER alone
     * would accept a client that sends nothing
     */
    int mode = require_client_cert
                   ? SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT
                   : SSL_VERIFY_NONE;
    SSL_CTX_set_verify(ctx, mode, client_certificate_verify);
    SSL_CTX_set_ecdh_auto(ctx, 1);

    if (SSL_CTX_use_certificate_chain_file(ctx, cert) <= 0) {
        ERR_print_errors_fp(stderr);
        exit(EXIT_FAILURE);
    }

    if (SSL_CTX_use_PrivateKey_file(ctx, key, SSL_FILETYPE_PEM) <= 0) {
        ERR_print_errors_fp(stderr);
        exit(EXIT_FAILURE);
    }

    /* verify private key */
    if (!SSL_CTX_check_private_key(ctx)) {
        fprintf(stderr, "Private key does not match the public certificate\n");
        exit(EXIT_FAILURE);
    }
}

/*
 * Wrap an accepted descriptor in a new SSL object and drive the server side
 * of the handshake. The socket is non-blocking, so a WANT_READ/WANT_WRITE
 * here is the normal case (the ClientHello has not arrived yet, or its reply
 * does not fit): OpenSSL resumes the handshake transparently on the next
 * SSL_read()/SSL_write(), and this function returns the SSL object so the
 * connection can be registered. A NULL return means the handshake really
 * failed (bad certificate, unsupported version, allocation failure) and the
 * caller must reject the connection instead of registering a socket whose
 * every future I/O would error out
 */
SSL *ssl_accept(SSL_CTX *ctx, int fd)
{
    SSL *ssl = SSL_new(ctx);
    if (!ssl) {
        ERR_print_errors_fp(stderr);
        return NULL;
    }
    if (!SSL_set_fd(ssl, fd)) {
        ERR_print_errors_fp(stderr);
        SSL_free(ssl);
        return NULL;
    }
    SSL_set_accept_state(ssl);
    ERR_clear_error();
    if (SSL_accept(ssl) <= 0) {
        int err = SSL_get_error(ssl, 0);
        if (err != SSL_ERROR_WANT_READ && err != SSL_ERROR_WANT_WRITE) {
            ERR_print_errors_fp(stderr);
            SSL_free(ssl);
            return NULL;
        }
    }
    return ssl;
}

ssize_t ssl_send_bytes(SSL *ssl, const unsigned char *buf, size_t len)
{

    size_t total     = 0;
    size_t bytesleft = len;
    ssize_t n        = 0;

    ERR_clear_error();

    while (total < len) {
        if ((n = SSL_write(ssl, buf + total, bytesleft)) <= 0) {
            int err = SSL_get_error(ssl, n);
            /*
             * The socket is non-blocking: SSL_ERROR_WANT_WRITE means the
             * kernel buffer is full (and WANT_READ means the handshake or a
             * renegotiation needs a packet back). Returning to the event loop
             * and waiting for the next EPOLLOUT/EPOLLIN is the only correct
             * move: the old code looped here, and since SSL_write() returns
             * immediately with the same code, a slow or silent peer pinned
             * the thread at 100% CPU forever, stalling every other connection
             * on the same loop. Report it as EAGAIN so the caller keeps the
             * unwritten tail and retries on the next event, like the plain
             * send_bytes() does
             */
            if (err == SSL_ERROR_WANT_WRITE || err == SSL_ERROR_WANT_READ) {
                errno = EAGAIN;
                break;
            }
            if (err == SSL_ERROR_ZERO_RETURN ||
                (err == SSL_ERROR_SYSCALL && !errno))
                return 0; // Connection closed
            /*
             * OpenSSL 3.0 reports a peer that vanished without the TLS
             * close_notify alert as SSL_ERROR_SSL with this reason. For a
             * *write* this is a dead connection, not an EOF: it must leave
             * here as -1 with a real errno, because write_data() only takes
             * the disconnect branch when wrote < 0 and errno is not EAGAIN.
             * Returning 0 instead would make it assume nothing was
             * available to write, reset towrite and silently discard the
             * whole pending buffer. The old fall-through did return -1 but
             * with errno still 0, so the log line read "error sending data:
             * Success"
             */
#ifdef SSL_R_UNEXPECTED_EOF_WHILE_READING
            if (err == SSL_ERROR_SSL &&
                ERR_GET_REASON(ERR_peek_last_error()) ==
                    SSL_R_UNEXPECTED_EOF_WHILE_READING)
                errno = ECONNRESET;
#endif
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            else
                goto err;
        }
        total += n;
        bytesleft -= n;
    }

    return total;

err:

    fprintf(stderr, "SSL_write(2) - error sending data: %s\n", strerror(errno));
    return -1;
}

ssize_t ssl_recv_bytes(SSL *ssl, unsigned char *buf, size_t bufsize)
{

    ssize_t n     = 0;
    ssize_t total = 0;

    ERR_clear_error();

    while (total < (ssize_t)bufsize) {

        if ((n = SSL_read(ssl, buf, bufsize - total)) <= 0) {
            int err = SSL_get_error(ssl, n);
            /*
             * Same reasoning as ssl_send_bytes(): WANT_READ (no complete TLS
             * record has arrived yet) and WANT_WRITE (the reply needs a
             * flight out) must yield to the event loop instead of spinning,
             * with errno left as EAGAIN so recv_data()'s caller knows no
             * error happened and simply has to wait for more bytes
             */
            if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
                errno = EAGAIN;
                break;
            }
            if (err == SSL_ERROR_ZERO_RETURN ||
                (err == SSL_ERROR_SYSCALL && !errno))
                return 0; // Connection closed
            /*
             * OpenSSL 3.0 reports a peer that vanished without the TLS
             * close_notify alert as SSL_ERROR_SSL with this reason instead
             * of a plain EOF. A client that kills its process or drops the
             * network is the routine case for a broker: treating it as the
             * disconnect it is keeps recv_data()'s semantics (0 == peer
             * closed), where the old fall-through returned -1 with errno
             * still 0 and logged "error reading data: Success" before
             * reporting a socket error for what is a normal close
             */
#ifdef SSL_R_UNEXPECTED_EOF_WHILE_READING
            if (err == SSL_ERROR_SSL &&
                ERR_GET_REASON(ERR_peek_last_error()) ==
                    SSL_R_UNEXPECTED_EOF_WHILE_READING)
                return 0;
#endif
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            else
                goto err;
        }

        if (n == 0)
            return 0;

        buf += n;
        total += n;
    }

    return total;

err:

    fprintf(stderr, "SSL_read(2) - error reading data: %s\n", strerror(errno));
    return -1;
}

/*
 * Main connection functions, meant to be set as function pointer to a struct
 * connection handle
 */
static int conn_accept(struct connection *c, int fd)
{
    int ret = accept_conn(fd, c->ip);
    c->fd   = ret;
    return ret;
}

static ssize_t conn_send(struct connection *c, const unsigned char *buf,
                         size_t len)
{
    return send_bytes(c->fd, buf, len);
}

static ssize_t conn_recv(struct connection *c, unsigned char *buf, size_t len)
{
    return recv_bytes(c->fd, buf, len);
}

static void conn_close(struct connection *c)
{
    if (c->fd >= 0 && close(c->fd) < 0)
        perror("close");
    c->fd = -1;
}

// TLS version of the connection functions
// XXX Not so neat, improve later
static int conn_tls_accept(struct connection *c, int serverfd)
{
    int fd = accept_conn(serverfd, c->ip);
    if (fd < 0)
        return fd;
    c->ssl = ssl_accept(c->ctx, fd);
    if (!c->ssl) {
        /*
         * The handshake failed outright: drop the descriptor and report the
         * failure like a rejected accept, or the caller would register a
         * client that can never complete a TLS exchange
         */
        if (close(fd) < 0)
            perror("close");
        c->fd = -1;
        return -1;
    }
    c->fd  = fd;
    return fd;
}

static ssize_t conn_tls_send(struct connection *c, const unsigned char *buf,
                             size_t len)
{
    return ssl_send_bytes(c->ssl, buf, len);
}

static ssize_t conn_tls_recv(struct connection *c, unsigned char *buf,
                             size_t len)
{
    return ssl_recv_bytes(c->ssl, buf, len);
}

static void conn_tls_close(struct connection *c)
{
    if (c->ssl) {
        /*
         * Send the close_notify before releasing the object: skipping the
         * shutdown makes the peer see an unclean close (it cannot tell a
         * truncated record from a real one). A best effort is enough here,
         * the descriptor is about to be closed anyway
         */
        SSL_shutdown(c->ssl);
        SSL_free(c->ssl);
        c->ssl = NULL;
    }
    if (c->fd >= 0 && close(c->fd) < 0)
        perror("close");
}

void connection_init(struct connection *conn, const SSL_CTX *ssl_ctx)
{
    conn->fd  = -1;
    conn->ssl = NULL; // Will be filled in case of TLS connection on accept
    conn->ctx = (SSL_CTX *)ssl_ctx;
    if (ssl_ctx) {
        // We need a TLS connection
        conn->accept = conn_tls_accept;
        conn->send   = conn_tls_send;
        conn->recv   = conn_tls_recv;
        conn->close  = conn_tls_close;
    } else {
        conn->accept = conn_accept;
        conn->send   = conn_send;
        conn->recv   = conn_recv;
        conn->close  = conn_close;
    }
}

/*
 * Simple abstraction over a socket connection, based on the connection type,
 * sets plain accept, read, write and close functions or the TLS version one.
 *
 * This structure allows to ignore some details at a higher level where we can
 * simply call accept, send, recv or close without actually worrying of the
 * type of the underlying communication.
 */
struct connection *connection_new(const SSL_CTX *ssl_ctx)
{
    struct connection *conn = try_alloc(sizeof(*conn));
    connection_init(conn, ssl_ctx);
    return conn;
}

/*
 * 4 connection type agnostic functions to be used at a higher level, like the
 * server module. They accept a connection structure as the first parameter
 * in order to leverage the previously set underlying function.
 */
int accept_connection(struct connection *c, int fd) { return c->accept(c, fd); }

ssize_t send_data(struct connection *c, const unsigned char *buf, size_t len)
{
    return c->send(c, buf, len);
}

ssize_t recv_data(struct connection *c, unsigned char *buf, size_t len)
{
    return c->recv(c, buf, len);
}

void close_connection(struct connection *c) { c->close(c); }
