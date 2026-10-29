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

#include "handlers.h"
#include "handlers_internal.h"
#include "acl.h"
#include "config.h"
#include "logging.h"
#include "memory.h"
#include "mqtt.h"
#include "persistence.h"
#include "server.h"
#include "sol_internal.h"
#include "topic_match.h"
#include <stdio.h>
#include <time.h>

/* Prototype for a command handler */
typedef int handler(struct io_event *);

static int pingreq_handler(struct io_event *);
static int protocol_violation_handler(struct io_event *);

/*
 * Reserve `len` bytes at the end of the client's write buffer, returns true
 * when the packet fits. Every reply is packed straight into that buffer, of
 * exactly conf->max_request_size bytes: the periodic retransmission scan can
 * fill it close to the brim while the client keeps sending commands, and an
 * unguarded pack would then write past the end of the heap block. Callers
 * must skip the reply when this returns false (a dropped reply is recovered
 * by the sender retransmitting, an overflow is not)
 */
bool outgoing_reserve(struct client *c, size_t len)
{
    struct config *conf = c->server->conf;
    if (c->towrite + len > conf->max_request_size) {
        log_error("Cannot send reply to %s: write buffer full (towrite = "
                  "%zu, packet = %zu bytes)",
                  c->client_id, c->towrite, len);
        return false;
    }
    /*
     * Grow the buffer to the next power of two that fits, capped at
     * max_request_size. A connection that only exchanges small packets never
     * allocates more than the initial size; the old code allocated the whole
     * max_request_size (512KB by default, 2MB in the shipped configuration)
     * for both buffers the moment the socket was accepted
     */
    if (c->towrite + len <= c->wbuf_cap)
        return true;
    size_t cap = c->wbuf_cap ? c->wbuf_cap : INITIAL_BUFFER_SIZE;
    while (cap < c->towrite + len)
        cap *= 2;
    if (cap > conf->max_request_size)
        cap = conf->max_request_size;
    c->wbuf     = try_realloc(c->wbuf, cap);
    c->wbuf_cap = cap;
    return true;
}

/*
 * Duplicate `len` bytes into a fresh NUL terminated buffer. The MQTT strings
 * this is used on (the will topic and message) are binary: an application
 * message may carry any byte, NUL included, so the copy is driven by the
 * decoded length and never by strlen(), which would truncate it
 */
unsigned char *dup_bytes(const unsigned char *src, size_t len)
{
    unsigned char *dst = try_alloc(len + 1);
    if (src && len > 0)
        memcpy(dst, src, len);
    dst[len] = '\0';
    return dst;
}

/*
 * Deep copy of a PUBLISH packet: the copy owns its own topic and payload
 * buffers and comes with a single reference held by the caller (DECREF
 * releases it). The retained message of a topic needs exactly this, a packet
 * that outlives the one travelling to the current subscribers, so that every
 * later subscription can be served from it
 */
struct mqtt_packet *mqtt_publish_copy(const struct mqtt_packet *src)
{
    struct mqtt_packet *copy = mqtt_packet_alloc(src->header.byte);

    mqtt_packet_copy(copy, src);
    /* The caller owns the copy from here on */
    INCREF(copy, struct mqtt_packet);
    return copy;
}

/*
 * Command handler mapped usign their position paired with their type, one
 * slot for each of the 16 possible values of the 4 bits packet type, so that
 * an unexpected type can never index past the end of the table. Slots that
 * don't hold a real handler point to protocol_violation_handler instead of
 * NULL.
 */
static handler *handlers[16] = {
    protocol_violation_handler, /* 0:  reserved */
    connect_handler,            /* 1:  CONNECT    (client to server) */
    protocol_violation_handler, /* 2:  CONNACK    (server to client) */
    publish_handler,            /* 3:  PUBLISH    (client to server) */
    puback_handler,             /* 4:  PUBACK     (client to server) */
    pubrec_handler,             /* 5:  PUBREC     (client to server) */
    pubrel_handler,             /* 6:  PUBREL     (client to server) */
    pubcomp_handler,            /* 7:  PUBCOMP    (client to server) */
    subscribe_handler,          /* 8:  SUBSCRIBE  (client to server) */
    protocol_violation_handler, /* 9:  SUBACK     (server to client) */
    unsubscribe_handler,        /* 10: UNSUBSCRIBE(client to server) */
    protocol_violation_handler, /* 11: UNSUBACK   (server to client) */
    pingreq_handler,            /* 12: PINGREQ    (client to server) */
    protocol_violation_handler, /* 13: PINGRESP   (server to client) */
    disconnect_handler,         /* 14: DISCONNECT (client to server) */
    protocol_violation_handler  /* 15: reserved */
};

static int pingreq_handler(struct io_event *e)
{
    log_debug("Received PINGREQ from %s", e->client->client_id);
    e->data.header.byte = PINGRESP_B;
#if THREADSNR > 0
    pthread_mutex_lock(&e->client->mutex);
#endif
    if (outgoing_reserve(e->client, MQTT_HEADER_LEN)) {
        mqtt_pack(&e->data, e->client->wbuf + e->client->towrite);
        e->client->towrite += MQTT_HEADER_LEN;
    }
#if THREADSNR > 0
    pthread_mutex_unlock(&e->client->mutex);
#endif
    log_debug("Sending PINGRESP to %s", e->client->client_id);
    return REPLY;
}

/*
 * A client sent a packet that only the broker is allowed to send (or a
 * reserved type): there's no handler for it, log the violation and ask the
 * caller to drop the connection instead of dereferencing a NULL slot.
 */
static int protocol_violation_handler(struct io_event *e)
{
    log_error("Protocol violation: packet type %u received from %s is only "
              "valid server to client",
              e->data.header.bits.type, e->client->client_id);
    return -ERRPACKETERR;
}

/*
 * This is the only public API we expose from this module beside
 * publish_message. It just give access to handlers mapped by message type.
 */
int handle_command(unsigned type, struct io_event *event)
{
    /* The type is 4 bits wide, still never index the table out of bounds */
    if (type >= sizeof(handlers) / sizeof(*handlers))
        return protocol_violation_handler(event);

    /*
     * MQTT v3.1.1 §3.1: the first packet a client sends MUST be a CONNECT
     * (and a client whose CONNECT was refused never becomes connected).
     * Without this guard a SUBSCRIBE sent before CONNECT would reach a
     * handler that dereferences the still missing session: a remote client
     * could crash the broker without authenticating, and a PUBLISH sent
     * before CONNECT would sidestep the allow_anonymous check entirely
     */
    if (type != CONNECT && event->client->connected == false) {
        log_error("Protocol violation: packet type %u received from %s "
                  "before CONNECT",
                  type, event->client->client_id[0] ? event->client->client_id
                                                    : "unknown");
        return -ERRPACKETERR;
    }

    return handlers[type](event);
}
