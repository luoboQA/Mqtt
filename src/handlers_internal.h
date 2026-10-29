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

#ifndef HANDLERS_INTERNAL_H
#define HANDLERS_INTERNAL_H

#include "handlers.h"
#include "mqtt.h"
#include <stdbool.h>
#include <stddef.h>

/*
 * Prototypes shared by the modules the command handling used to live in
 * (handlers.c, session.c, connect.c, subscribe.c, publish.c). Everything here
 * is internal to the broker: the public entry points stay in handlers.h
 */

struct client;
struct client_session;
struct io_event;
struct mqtt_packet;
struct server;
struct topic;

/* The command handlers the dispatch table in handlers.c points at */
int connect_handler(struct io_event *);
int disconnect_handler(struct io_event *);
int subscribe_handler(struct io_event *);
int unsubscribe_handler(struct io_event *);
int publish_handler(struct io_event *);
int puback_handler(struct io_event *);
int pubrec_handler(struct io_event *);
int pubrel_handler(struct io_event *);
int pubcomp_handler(struct io_event *);

/* Packet copies shared by the connect, subscribe and publish paths */
unsigned char *dup_bytes(const unsigned char *, size_t);
struct mqtt_packet *mqtt_publish_copy(const struct mqtt_packet *);

/* Session construction and inbound QoS 2 bookkeeping */
struct client_session *client_session_alloc(struct server *, const char *);
bool inbound_q2_find(const struct client_session *, unsigned short);
void inbound_q2_track(struct client_session *, unsigned short);
void inbound_q2_untrack(struct client_session *, unsigned short);
void session_track_topic(struct client_session *, struct topic *);

/* Session inflight bookkeeping used by the publish and subscribe paths */
unsigned short next_free_mid(struct client_session *);
void inflight_msg_init(struct client_session *, unsigned short,
                       struct mqtt_packet *);
void inflight_ack_init(struct client_session *, unsigned short);
void inflight_msg_remove(struct client_session *, unsigned short);
void queue_offline_msg(struct client_session *, struct mqtt_packet *, u16);
bool inflight_window_full(const struct client_session *);

#endif
