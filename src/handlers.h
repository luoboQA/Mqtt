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

#ifndef HANDLERS_H
#define HANDLERS_H

#include <stdbool.h>
#include <stddef.h>

struct topic;
struct mqtt_packet;
struct io_event;
struct client_session;

int publish_message(struct mqtt_packet *, const struct topic *);

/*
 * Create a persistent session and register it in the server map, the entry
 * point the persistence loader uses to rebuild what it read back from disk.
 * Meant to run before the event loops start: it takes no lock
 */
struct client_session *session_restore(const char *session_id);

/*
 * Register `filter` for `session`, exactly like one tuple of a SUBSCRIBE
 * packet would (the filter is validated and normalized here). The caller must
 * hold the global mutex; returns false when the filter is malformed
 */
bool session_subscribe(struct client_session *, const char *filter,
                       unsigned char qos);

/*
 * Queue a message for a session that is not connected, at QoS > 0: the packet
 * is built here, takes an identifier of its own and is registered in the
 * session inflight tables, so a later reconnect replays it. The caller must
 * hold the global mutex
 */
void session_queue_message(struct client_session *, const char *topic,
                           unsigned char qos, const unsigned char *payload,
                           size_t len);

/*
 * Drop the oldest message queued for a disconnected session, releasing the
 * packet, its inflight entry and the queue slot: what the max_memory policy
 * does to give memory back. Returns false when the queue is already empty
 */
bool session_drop_oldest_queued(struct client_session *);

/*
 * Deliver an LWT message through the same normalized path a normal PUBLISH
 * takes (trailing '/', wildcard subscriber attach): acquires the locks it
 * needs itself
 */
void publish_lwt(struct mqtt_packet *);

int handle_command(unsigned, struct io_event *);

#endif
