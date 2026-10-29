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

#include "proto_test.h"
#include "unit.h"
#include "../src/memory.h"
#include "../src/mqtt.h"
#include <stdio.h>
#include <string.h>

/*
 * A PUBLISH is handed to mqtt_unpack() with `buf` already past the fixed
 * header, exactly as process_message() does: `byte` is the first byte of the
 * packet and `len` is the Remaining Length.
 */
static char *test_unpack_publish_qos0(void)
{
    u8 wire[] = {0x30, 0x07, 0x00, 0x03, 'a', '/', 'b', 'h', 'i'};
    struct mqtt_packet pkt = {0};

    int rc = mqtt_unpack(wire + 2, &pkt, wire[0], 7);
    ASSERT("mqtt::unpack_publish_qos0...FAIL", rc == MQTT_OK);
    ASSERT("mqtt::unpack_publish_qos0 topic...FAIL",
           pkt.publish.topiclen == 3 &&
               memcmp(pkt.publish.topic, "a/b", 3) == 0);
    ASSERT("mqtt::unpack_publish_qos0 payload...FAIL",
           pkt.publish.payloadlen == 2 &&
               memcmp(pkt.publish.payload, "hi", 2) == 0);
    mqtt_packet_destroy(&pkt);
    printf("mqtt::unpack_publish_qos0...OK\n");
    return 0;
}

static char *test_unpack_publish_qos1(void)
{
    u8 wire[] = {0x32, 0x09, 0x00, 0x03, 'a', '/', 'b',
                 0x00, 0x09, 'h',  'i'};
    struct mqtt_packet pkt = {0};

    int rc = mqtt_unpack(wire + 2, &pkt, wire[0], 9);
    ASSERT("mqtt::unpack_publish_qos1...FAIL", rc == MQTT_OK);
    ASSERT("mqtt::unpack_publish_qos1 mid...FAIL", pkt.publish.pkt_id == 9);
    ASSERT("mqtt::unpack_publish_qos1 qos...FAIL",
           pkt.header.bits.qos == AT_LEAST_ONCE);
    mqtt_packet_destroy(&pkt);
    printf("mqtt::unpack_publish_qos1...OK\n");
    return 0;
}

static char *test_pack_unpack_roundtrip(void)
{
    struct mqtt_packet out = {0};
    out.header.byte        = 0x32; /* PUBLISH, QoS 1 */
    out.publish            = (struct mqtt_publish){
        .pkt_id     = 9,
        .topiclen   = 3,
        .topic      = (u8 *)"a/b",
        .payloadlen = 2,
        .payload    = (u8 *)"hi"};

    u8 wire[64];
    usize size = mqtt_size(&out, NULL);
    ASSERT("mqtt::pack_size...FAIL", size == 11);
    ASSERT("mqtt::pack_write...FAIL", mqtt_pack(&out, wire) == size);

    struct mqtt_packet back = {0};
    int rc = mqtt_unpack(wire + 2, &back, wire[0], size - 2);
    ASSERT("mqtt::roundtrip...FAIL", rc == MQTT_OK);
    ASSERT("mqtt::roundtrip topic...FAIL",
           back.publish.topiclen == 3 &&
               memcmp(back.publish.topic, "a/b", 3) == 0);
    ASSERT("mqtt::roundtrip payload...FAIL",
           back.publish.payloadlen == 2 &&
               memcmp(back.publish.payload, "hi", 2) == 0);
    ASSERT("mqtt::roundtrip mid...FAIL", back.publish.pkt_id == 9);
    mqtt_packet_destroy(&back);
    printf("mqtt::pack_unpack_roundtrip...OK\n");
    return 0;
}

static char *test_reject_wildcard_topic(void)
{
    u8 wire[] = {0x30, 0x07, 0x00, 0x03, 'a', '#', 'b', 'h', 'i'};
    struct mqtt_packet pkt = {0};
    ASSERT("mqtt::reject_wildcard_topic...FAIL",
           mqtt_unpack(wire + 2, &pkt, wire[0], 7) != MQTT_OK);
    printf("mqtt::reject_wildcard_topic...OK\n");
    return 0;
}

static char *test_reject_empty_topic(void)
{
    u8 wire[] = {0x30, 0x04, 0x00, 0x00, 'h', 'i'};
    struct mqtt_packet pkt = {0};
    ASSERT("mqtt::reject_empty_topic...FAIL",
           mqtt_unpack(wire + 2, &pkt, wire[0], 4) != MQTT_OK);
    printf("mqtt::reject_empty_topic...OK\n");
    return 0;
}

static char *test_reject_qos3(void)
{
    u8 wire[] = {0x36, 0x07, 0x00, 0x03, 'a', '/', 'b', 'h', 'i'};
    struct mqtt_packet pkt = {0};
    ASSERT("mqtt::reject_qos3...FAIL",
           mqtt_unpack(wire + 2, &pkt, wire[0], 7) != MQTT_OK);
    printf("mqtt::reject_qos3...OK\n");
    return 0;
}

static char *test_reject_pingreq_with_body(void)
{
    u8 wire[] = {0xC0, 0x00, 0x00};
    struct mqtt_packet pkt = {0};
    ASSERT("mqtt::reject_pingreq_with_body...FAIL",
           mqtt_unpack(wire + 2, &pkt, wire[0], 1) != MQTT_OK);
    ASSERT("mqtt::accept_pingreq...FAIL",
           mqtt_unpack(wire + 2, &pkt, wire[0], 0) == MQTT_OK);
    printf("mqtt::reject_pingreq_with_body...OK\n");
    return 0;
}

static char *test_reject_server_packet_type(void)
{
    u8 wire[] = {0x20, 0x02, 0x00, 0x00}; /* CONNACK is server-to-client */
    struct mqtt_packet pkt = {0};
    ASSERT("mqtt::reject_server_packet_type...FAIL",
           mqtt_unpack(wire + 2, &pkt, wire[0], 2) != MQTT_OK);
    printf("mqtt::reject_server_packet_type...OK\n");
    return 0;
}

char *mqtt_test(void)
{
    RUN_TEST(test_unpack_publish_qos0);
    RUN_TEST(test_unpack_publish_qos1);
    RUN_TEST(test_pack_unpack_roundtrip);
    RUN_TEST(test_reject_wildcard_topic);
    RUN_TEST(test_reject_empty_topic);
    RUN_TEST(test_reject_qos3);
    RUN_TEST(test_reject_pingreq_with_body);
    RUN_TEST(test_reject_server_packet_type);
    return 0;
}
