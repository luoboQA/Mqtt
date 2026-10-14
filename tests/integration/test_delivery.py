import socket
import unittest
import sol_test
import base_testcase


class TestDelivery(base_testcase.BaseTestcase):
    """End to end delivery: subscriptions, QoS handshakes, retransmission,
    retained messages and session resume. Every case finishes with a liveness
    check so a broker crash cannot hide behind a passing test"""

    def connect(self, client_id, clean_session=True):
        conn = self.get_connection()
        conn.settimeout(3)
        conn.sendall(sol_test.create_connect(client_id,
                                             clean_session=clean_session))
        packet = conn.recv(100)
        connack, rc = sol_test.read_connack(packet)
        self.assertEqual(connack, 32)
        self.assertEqual(rc, 0)
        return conn, packet

    def subscribe(self, conn, mid, topics):
        conn.sendall(sol_test.create_subscribe(mid, topics))
        code, ack_mid, granted = sol_test.read_suback(conn.recv(100))
        self.assertEqual(code, 0x90)
        self.assertEqual(ack_mid, mid)

    def assert_broker_alive(self):
        conn, _ = self.connect('alive-check')
        conn.close()

    def test_wildcard_subscribe_after_exact(self):
        """"a" and "a/#" live on the very same node of the topic tree:
        subscribing the bare topic first used to silently drop the wildcard
        filter, no message under the prefix was ever delivered"""
        sub, _ = self.connect('wild-sub')
        try:
            self.subscribe(sub, 1, {'a': 0})
            self.subscribe(sub, 2, {'a/#': 0})
            pub, _ = self.connect('wild-pub')
            try:
                pub.sendall(sol_test.create_publish('a/x', 'under-a'))
                header, topic, payload, _ = sol_test.read_publish(
                    sub.recv(100))
                self.assertEqual(topic, 'a/x')
                self.assertEqual(payload, b'under-a')
                # The bare topic still matches its own filter, delivered once
                pub.sendall(sol_test.create_publish('a', 'exact-a'))
                header, topic, payload, _ = sol_test.read_publish(
                    sub.recv(100))
                self.assertEqual(topic, 'a')
                self.assertEqual(payload, b'exact-a')
            finally:
                pub.close()
        finally:
            sub.close()
        self.assert_broker_alive()

    def test_retained_message(self):
        pub, _ = self.connect('ret-pub')
        try:
            # QoS 1 so the PUBACK tells us the broker has stored the
            # message: a QoS 0 fire-and-forget would race the SUBSCRIBE
            # of the second connection (no ordering across connections)
            pub.sendall(sol_test.create_publish('ret/topic', 'keep-me',
                                                qos=1, mid=8, retain=True))
            self.assertEqual(pub.recv(100)[0], 0x40)  # PUBACK
        finally:
            pub.close()
        # A subscriber arriving later still gets the retained message
        sub, _ = self.connect('ret-sub')
        try:
            sub.sendall(sol_test.create_subscribe(1, {'ret/topic': 0}))
            # The retained message is packed into the same write buffer of
            # the SUBACK, so both can land in a single TCP segment: keep
            # reading until something follows the SUBACK
            sub.settimeout(3)
            buf = b''
            while (len(buf) < 2 or len(buf) < 2 + buf[1]
                   or not buf[2 + buf[1]:]):
                buf += sub.recv(2048)
            rest = buf[2 + buf[1]:]
            code, ack_mid, _ = sol_test.read_suback(buf[:2 + buf[1]])
            self.assertEqual(code, 0x90)
            self.assertEqual(ack_mid, 1)
            header, topic, payload, _ = sol_test.read_publish(rest)
            self.assertEqual(topic, 'ret/topic')
            self.assertEqual(payload, b'keep-me')
            self.assertEqual(header & 0x01, 1)  # retain flag echoed
        finally:
            sub.close()
        self.assert_broker_alive()

    def test_qos_one_roundtrip(self):
        sub, _ = self.connect('qos1-sub')
        try:
            self.subscribe(sub, 1, {'q1/topic': 1})
            pub, _ = self.connect('qos1-pub')
            try:
                pub.sendall(sol_test.create_publish('q1/topic', 'hello',
                                                    qos=1, mid=7))
                # The broker acknowledges the publisher with a PUBACK
                ack = pub.recv(100)
                self.assertEqual(ack[0], 0x40)
                self.assertEqual(ack[2:4], b'\x00\x07')
            finally:
                pub.close()
            # The subscriber got a QoS 1 copy and acknowledges it
            header, topic, payload, mid = sol_test.read_publish(sub.recv(100))
            self.assertEqual((header >> 1) & 0x03, 1)
            self.assertEqual(topic, 'q1/topic')
            self.assertEqual(payload, b'hello')
            sub.sendall(sol_test.create_puback(mid))
        finally:
            sub.close()
        self.assert_broker_alive()

    def test_qos_one_retransmit_sets_dup(self):
        """A QoS 1 message the subscriber never acknowledges is re-sent
        after retransmit_interval with the DUP flag set"""
        sub, _ = self.connect('dup-sub')
        try:
            self.subscribe(sub, 1, {'resend/topic': 1})
            pub, _ = self.connect('dup-pub')
            try:
                pub.sendall(sol_test.create_publish('resend/topic', 'twice',
                                                    qos=1, mid=3))
                self.assertEqual(pub.recv(100)[0], 0x40)  # PUBACK
            finally:
                pub.close()
            first = sub.recv(100)
            header, topic, payload, mid = sol_test.read_publish(first)
            self.assertEqual(payload, b'twice')
            self.assertEqual(header & 0x08, 0)  # not a duplicate yet
            # No PUBACK: wait for the broker to re-send with DUP
            sub.settimeout(8)
            second = sub.recv(100)
            header, topic, payload, mid2 = sol_test.read_publish(second)
            self.assertEqual(payload, b'twice')
            self.assertEqual(mid2, mid)  # the packet identifier is stable
            self.assertNotEqual(header & 0x08, 0)
        finally:
            sub.close()
        self.assert_broker_alive()

    def test_qos_two_pubrel_retransmit(self):
        """The QoS 2 handshake: PUBLISH -> PUBREC -> PUBREL -> PUBCOMP. A
        PUBREL the subscriber never answers is re-sent after
        retransmit_interval (this path used to dereference a NULL packet
        on every retransmission and crash the broker)"""
        sub, _ = self.connect('qos2-sub')
        try:
            self.subscribe(sub, 1, {'q2/topic': 2})
            pub, _ = self.connect('qos2-pub')
            try:
                pub.sendall(sol_test.create_publish('q2/topic', 'exactly',
                                                    qos=2, mid=9))
                self.assertEqual(pub.recv(100)[0], 0x50)  # PUBREC
                pub.sendall(sol_test.create_pubrel(9))
                self.assertEqual(pub.recv(100)[0], 0x70)  # PUBCOMP
            finally:
                pub.close()
            # Publisher handshake done, the QoS 2 copy is delivered
            header, topic, payload, mid = sol_test.read_publish(sub.recv(100))
            self.assertEqual((header >> 1) & 0x03, 2)
            self.assertEqual(payload, b'exactly')
            sub.sendall(sol_test.create_pubrec(mid))
            self.assertEqual(sub.recv(100)[0], 0x62)  # PUBREL
            # Withhold the PUBCOMP: the PUBREL must come back, well formed
            sub.settimeout(8)
            again = sub.recv(100)
            self.assertEqual(again[0], 0x62)
            self.assertEqual(again[2:4], bytes([mid >> 8, mid & 0xFF]))
        finally:
            sub.close()
        self.assert_broker_alive()

    def test_qos_two_counter_returns_to_zero(self):
        """Two consecutive QoS 2 handshakes on the same subscriber. PUBREC
        releases the stored PUBLISH, so by the time PUBCOMP lands the inflight
        entry holds no packet: the completion must still account for it (via
        the PUBREL-pending marker PUBREC armed) or has_inflight() stays set
        forever and the periodic scan keeps walking a session with nothing
        in flight"""
        sub, _ = self.connect('qos2-leak-sub')
        try:
            self.subscribe(sub, 1, {'leak/topic': 2})
            pub, _ = self.connect('qos2-leak-pub')
            try:
                for i, payload in enumerate((b'first', b'second'), start=1):
                    pub.sendall(sol_test.create_publish('leak/topic', payload,
                                                        qos=2, mid=10 + i))
                    self.assertEqual(pub.recv(100)[0], 0x50)  # PUBREC
                    pub.sendall(sol_test.create_pubrel(10 + i))
                    self.assertEqual(pub.recv(100)[0], 0x70)  # PUBCOMP
                    header, topic, got, mid = sol_test.read_publish(
                        sub.recv(100))
                    self.assertEqual((header >> 1) & 0x03, 2)
                    self.assertEqual(got, payload)
                    sub.sendall(sol_test.create_pubrec(mid))
                    self.assertEqual(sub.recv(100)[0], 0x62)  # PUBREL
                    sub.sendall(sol_test.create_pubcomp(mid))
            finally:
                pub.close()
        finally:
            sub.close()
        self.assert_broker_alive()

    def test_persistent_session_offline_queue(self):
        """A message published while a persistent (clean_session=false)
        subscriber is away is queued and delivered on reconnect, together
        with a CONNACK carrying the session-present flag"""
        sub, _ = self.connect('persist', clean_session=False)
        self.subscribe(sub, 1, {'off/topic': 1})
        sub.sendall(sol_test.create_disconnect(0))
        sub.close()
        # Publish while the subscriber is away
        pub, _ = self.connect('off-pub')
        try:
            pub.sendall(sol_test.create_publish('off/topic', 'queued',
                                                qos=1, mid=4))
            self.assertEqual(pub.recv(100)[0], 0x40)  # PUBACK
        finally:
            pub.close()
        # Reconnect: the session is resumed and the queue flushed
        sub, packet = self.connect('persist', clean_session=False)
        try:
            self.assertEqual(packet[2] & 0x01, 1)  # session present
            # The queued message is packed right after the CONNACK: it may
            # share the very same segment the CONNACK arrived in
            buf = packet[4:]
            while not buf:
                buf = sub.recv(2048)
            header, topic, payload, mid = sol_test.read_publish(buf)
            self.assertEqual(topic, 'off/topic')
            self.assertEqual(payload, b'queued')
        finally:
            sub.close()
        self.assert_broker_alive()

    def test_duplicate_client_id_kicks_old(self):
        """MQTT v3.1.1 §3.1.4-2: a second CONNECT with the same id drops
        the connection that held it before"""
        first, _ = self.connect('dup-id')
        try:
            second, _ = self.connect('dup-id')
            try:
                # The old connection must be closed by the broker
                first.settimeout(3)
                try:
                    dropped = first.recv(100)
                    self.assertEqual(dropped, b'')
                except (ConnectionResetError, BrokenPipeError):
                    pass  # a reset is an acceptable way of dropping it
                # The new connection is fully functional
                second.sendall(sol_test.create_subscribe(1, {'kick': 0}))
                code, mid, granted = sol_test.read_suback(second.recv(100))
                self.assertEqual(code, 0x90)
            finally:
                second.close()
        finally:
            first.close()
        self.assert_broker_alive()

    def test_stats_topic(self):
        """The $SOL statistics are published on the interval set in the
        test configuration (1s) and are reachable through a wildcard"""
        sub, _ = self.connect('stats-sub')
        try:
            self.subscribe(sub, 1, {'$SOL/#': 0})
            sub.settimeout(5)
            header, topic, payload, _ = sol_test.read_publish(sub.recv(2048))
            self.assertTrue(topic.startswith('$SOL/broker/'))
        finally:
            sub.close()
        self.assert_broker_alive()
