import os
import socket
import struct
import time
import unittest
import sol_test
import base_testcase


class TestHardening(base_testcase.BaseTestcase):
    """Regression tests for the protocol gaps closed on top of the delivery
    and malformed suites: retained messages packed for the subscriber that
    receives them, the will delivered on any abnormal close, and the fixed
    header flags and length of the control packets. Every case finishes with
    a liveness check so a broker crash cannot hide behind a passing test"""

    def connect(self, client_id, clean_session=True, **kwargs):
        conn = self.get_connection()
        conn.settimeout(3)
        conn.sendall(sol_test.create_connect(client_id,
                                             clean_session=clean_session,
                                             **kwargs))
        packet = conn.recv(100)
        connack, rc = sol_test.read_connack(packet)
        self.assertEqual(connack, 32)
        self.assertEqual(rc, 0)
        return conn, packet

    def connect_resumed(self, client_id):
        """Reconnect a persistent session, returning the bytes the broker
        packed behind the CONNACK: the messages of the resumed session share
        its write buffer, so they can land in the very same TCP segment"""
        conn = self.get_connection()
        conn.settimeout(3)
        conn.sendall(sol_test.create_connect(client_id, clean_session=False))
        buf = b''
        while len(buf) < 4:
            buf += conn.recv(4096)
        connack, rc = sol_test.read_connack(buf[:4])
        self.assertEqual(connack, 32)
        self.assertEqual(rc, 0)
        return conn, buf[4:]

    def subscribe(self, conn, mid, topics):
        conn.sendall(sol_test.create_subscribe(mid, topics))
        code, ack_mid, _ = sol_test.read_suback(conn.recv(100))
        self.assertEqual(code, 0x90)
        self.assertEqual(ack_mid, mid)

    def subscribe_retained(self, conn, mid, topics):
        """Subscribe and return whatever the broker packed right behind the
        SUBACK: the retained messages share its write buffer, so they can land
        in the very same TCP segment"""
        conn.sendall(sol_test.create_subscribe(mid, topics))
        conn.settimeout(2)
        buf = b''
        while len(buf) < 2 or len(buf) < 2 + buf[1]:
            buf += conn.recv(2048)
        code, ack_mid, _ = sol_test.read_suback(buf[:2 + buf[1]])
        self.assertEqual(code, 0x90)
        self.assertEqual(ack_mid, mid)
        return buf[2 + buf[1]:]

    def read_ack(self, conn):
        """Read a 4 bytes acknowledgement, returning its type byte and the
        packet identifier as a full 16 bits value"""
        packet = conn.recv(100)
        self.assertEqual(len(packet), 4)
        return packet[0], struct.unpack('!H', packet[2:4])[0]

    def read_publishes(self, conn, count, buf=b''):
        """Read `count` PUBLISH packets out of the stream: the broker packs
        them back to back in its write buffer, so a single segment can carry
        several of them at once, and `buf` carries over whatever a previous
        read already pulled out of the socket"""
        out = []
        conn.settimeout(3)
        while len(out) < count:
            while len(out) < count:
                # Decode the Remaining Length from the bytes received so far,
                # stopping when the continuation bit says it is not over yet
                value, shift, lenbytes, complete = 0, 0, 0, False
                for byte in buf[1:5]:
                    value += (byte & 0x7F) << shift
                    shift += 7
                    lenbytes += 1
                    if not (byte & 0x80):
                        complete = True
                        break
                total = 1 + lenbytes + value
                if not complete or len(buf) < total:
                    break
                out.append(sol_test.read_publish(buf[:total]))
                buf = buf[total:]
            if len(out) < count:
                chunk = conn.recv(4096)
                self.assertNotEqual(chunk, b'', 'the broker closed the socket')
                buf += chunk
        return out

    def assert_broker_alive(self):
        conn, _ = self.connect('alive-check')
        self.send_disconnect(conn)
        conn.close()

    def assert_nothing_follows(self, conn):
        """No further byte must reach the client"""
        conn.settimeout(1)
        try:
            self.assertEqual(conn.recv(2048), b'')
        except socket.timeout:
            pass

    def expect_closed(self, conn):
        """The broker must drop the connection: a read returns EOF, or the
        peer resets it while our last bytes are still in flight"""
        conn.settimeout(2)
        try:
            return conn.recv(100) == b''
        except (ConnectionResetError, BrokenPipeError):
            return True
        except socket.timeout:
            return False

    def test_retained_delivered_at_subscription_qos(self):
        """A retained message is stored with its own QoS but delivered at the
        QoS the subscription was granted, capped by it (§4.4). It used to be
        replayed as the exact bytes the publisher sent, packet identifier
        included: a QoS 0 subscriber received a QoS 1 message carrying a
        random identifier it had never been assigned"""
        pub, _ = self.connect('retq-pub')
        try:
            pub.sendall(sol_test.create_publish('retq/t', 'keep', qos=1,
                                                mid=6, retain=True))
            self.assertEqual(self.read_ack(pub)[0], 0x40)  # PUBACK
        finally:
            pub.close()

        sub, _ = self.connect('retq-sub')
        try:
            rest = self.subscribe_retained(sub, 1, {'retq/t': 0})
            header, topic, payload, mid = sol_test.read_publish(rest)
            self.assertEqual(topic, 'retq/t')
            self.assertEqual(payload, b'keep')
            self.assertEqual(header & 0x01, 1)         # RETAIN kept, it is a
            self.assertEqual((header >> 1) & 0x03, 0)  # new subscription...
            self.assertIsNone(mid)                     # ...and QoS 0 has no id
        finally:
            sub.close()
        self.assert_broker_alive()

    def test_retained_overlapping_filters_delivered_once(self):
        """'retov/t' and 'retov/+' cover the very same topic: subscribing to
        both in a single packet must flush the retained message once, exactly
        like the live delivery path keeps a single subscriber entry per topic"""
        pub, _ = self.connect('retov-pub')
        try:
            pub.sendall(sol_test.create_publish('retov/t', 'once', qos=1,
                                                mid=2, retain=True))
            self.assertEqual(self.read_ack(pub)[0], 0x40)
        finally:
            pub.close()

        sub, _ = self.connect('retov-sub')
        try:
            rest = self.subscribe_retained(sub, 1, {'retov/t': 0, 'retov/+': 0})
            body, rl = sol_test.mqtt_decode_len(rest)
            consumed = 1 + (len(rest) - len(body)) + rl
            header, topic, payload, _ = sol_test.read_publish(rest)
            self.assertEqual(payload, b'once')
            self.assertEqual(rest[consumed:], b'')  # no second copy behind it
            self.assert_nothing_follows(sub)        # nor in a later segment
        finally:
            sub.close()
        self.assert_broker_alive()

    def test_retained_qos_flow_is_tracked(self):
        """A retained message delivered at QoS > 0 goes through the normal
        inflight bookkeeping: it takes an identifier of its own and is
        retransmitted until it is acknowledged (§4.4), instead of being a
        fire-and-forget copy of the stored bytes"""
        pub, _ = self.connect('retinf-pub')
        try:
            pub.sendall(sol_test.create_publish('retinf/t', 'kept', qos=2,
                                                mid=4, retain=True))
            self.assertEqual(self.read_ack(pub)[0], 0x50)  # PUBREC
            pub.sendall(sol_test.create_pubrel(4))
            self.assertEqual(self.read_ack(pub)[0], 0x70)  # PUBCOMP
        finally:
            pub.close()

        sub, _ = self.connect('retinf-sub')
        try:
            rest = self.subscribe_retained(sub, 1, {'retinf/t': 2})
            header, _, payload, mid = sol_test.read_publish(rest)
            self.assertEqual((header >> 1) & 0x03, 2)
            self.assertIsNotNone(mid)
            self.assertEqual(payload, b'kept')
            # Never acknowledged: the retransmission scan (2s in the test
            # configuration) has to send it again with DUP set and the very
            # same identifier
            time.sleep(2.6)
            header, _, _, retx_mid = self.read_publishes(sub, 1)[0]
            self.assertEqual(retx_mid, mid)
            self.assertEqual((header >> 3) & 0x01, 1)
            # Complete the handshake so the session drains
            sub.sendall(sol_test.create_pubrec(mid))
            self.assertEqual(self.read_ack(sub)[0], 0x62)  # PUBREL
            sub.sendall(sol_test.create_pubcomp(mid))
        finally:
            sub.close()
        self.assert_broker_alive()

    def test_offline_queue_keeps_publication_order(self):
        """Messages published while a persistent subscriber is away are handed
        back in the order they were published: the queue used to be filled on
        the head, so a reconnecting client received its backlog reversed"""
        # A session of its own per run: a client id left behind by an earlier
        # run would still hold its own backlog, which would then be the first
        # thing this one reads
        client_id = 'ord-sub-%d' % os.getpid()
        sub, _ = self.connect(client_id, clean_session=False)
        try:
            self.subscribe(sub, 1, {'ord/t': 1})
        finally:
            sub.close()  # the session stays behind
        time.sleep(0.2)

        pub, _ = self.connect('ord-pub')
        try:
            for index, message in enumerate((b'm1', b'm2', b'm3')):
                pub.sendall(sol_test.create_publish('ord/t', message, qos=1,
                                                    mid=10 + index))
                self.assertEqual(self.read_ack(pub)[0], 0x40)
        finally:
            pub.close()

        sub, leftover = self.connect_resumed(client_id)
        try:
            publishes = self.read_publishes(sub, 3, leftover)
            self.assertEqual([publish[2] for publish in publishes],
                             [b'm1', b'm2', b'm3'])
            # Acknowledge them so the queue drains before the session ends
            for _, _, _, mid in publishes:
                sub.sendall(sol_test.create_puback(mid))
        finally:
            sub.close()
        self.assert_broker_alive()

    def test_will_published_on_protocol_error(self):
        """§3.1.2.5: the will is published whenever the network connection is
        closed without a DISCONNECT, which includes the protocol errors (a
        packet only the server may send, a second CONNECT). It used to be
        delivered on read errors alone"""
        watcher, _ = self.connect('pe-watch')
        try:
            self.subscribe(watcher, 1, {'pe/will': 0})
            victim = self.get_connection()
            try:
                victim.settimeout(3)
                victim.sendall(sol_test.create_connect(
                    'pe-victim', will_topic='pe/will', will_payload=b'gone'))
                victim.recv(100)                                 # CONNACK
                victim.sendall(struct.pack('!BBH', 0x90, 2, 1))  # SUBACK
                self.assertTrue(self.expect_closed(victim))
                _, topic, payload, _ = sol_test.read_publish(watcher.recv(100))
                self.assertEqual(topic, 'pe/will')
                self.assertEqual(payload, b'gone')
            finally:
                victim.close()
        finally:
            watcher.close()
        self.assert_broker_alive()

    def test_will_payload_keeps_nul_bytes(self):
        """An application message is a binary blob, the will payload is one:
        its length comes from the packet, while a strlen() driven copy
        truncated it at the first NUL byte it carried"""
        watcher, _ = self.connect('wnul-watch')
        try:
            self.subscribe(watcher, 1, {'wnul/will': 0})
            victim = self.get_connection()
            try:
                victim.settimeout(3)
                victim.sendall(sol_test.create_connect(
                    'wnul-victim', will_topic='wnul/will',
                    will_payload=b'a\x00b'))
                victim.recv(100)
            finally:
                victim.close()  # abrupt: the will is published
            _, _, payload, _ = sol_test.read_publish(watcher.recv(100))
            self.assertEqual(payload, b'a\x00b')
        finally:
            watcher.close()
        self.assert_broker_alive()

    def test_will_topic_with_wildcard_rejected(self):
        """§3.1.3.2: the will topic is a Topic Name, so §4.7 applies and it
        carries no wildcard. Publishing one verbatim on an abrupt disconnect
        would hand the subscribers a topic nobody could have subscribed to"""
        conn = self.get_connection()
        try:
            conn.settimeout(2)
            conn.sendall(sol_test.create_connect('willbad', will_topic='bad/#',
                                                 will_payload=b'boom'))
            self.assertTrue(self.expect_closed(conn))
        finally:
            conn.close()
        self.assert_broker_alive()

    def test_subscribe_with_bad_flags_rejected(self):
        """§2.2.2: the reserved bits of a SUBSCRIBE are fixed to 0b0010, any
        other value is malformed and the connection has to be closed"""
        conn = self.get_connection()
        try:
            conn.settimeout(2)
            conn.sendall(sol_test.create_connect('flags-sub'))
            self.assertEqual(conn.recv(100)[0], 32)  # CONNACK
            # A well formed payload behind a malformed fixed header
            payload = struct.pack('!HH', 1, 1) + b'a' + struct.pack('!B', 0)
            conn.sendall(struct.pack('!BB', 0x81, len(payload)) + payload)
            self.assertTrue(self.expect_closed(conn))
        finally:
            conn.close()
        self.assert_broker_alive()

    def test_pingreq_with_payload_rejected(self):
        """§3.12.1: PINGREQ is the fixed header alone, a Remaining Length
        above 0 describes bytes that have no meaning"""
        conn = self.get_connection()
        try:
            conn.settimeout(2)
            conn.sendall(sol_test.create_connect('ping-payload'))
            self.assertEqual(conn.recv(100)[0], 32)  # CONNACK
            conn.sendall(struct.pack('!BBB', 0xC0, 1, 0x00))
            self.assertTrue(self.expect_closed(conn))
        finally:
            conn.close()
        self.assert_broker_alive()


if __name__ == '__main__':
    unittest.main()
