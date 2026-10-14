import struct
import socket
import unittest
import sol_test
import base_testcase


class TestMalformed(base_testcase.BaseTestcase):
    """A malformed or out-of-spec packet must never crash the broker: the
    offending connection is answered with an error or dropped, and every
    following client is still served"""

    def send_raw(self, payload):
        """Send raw bytes to the broker, drain whatever comes back (the
        broker may reply, close, or simply keep the connection open waiting
        for the rest of a partial packet) and close from our side"""
        conn = self.get_connection()
        conn.settimeout(2)
        try:
            conn.sendall(payload)
            self.drain(conn)
        finally:
            conn.close()

    def drain(self, conn):
        """Read until the broker closes or stops talking: a reset is the
        expected outcome of a dropped malformed connection (the broker
        closes while our bytes are still in flight)"""
        try:
            while conn.recv(1024):
                pass
        except socket.timeout:
            pass
        except (ConnectionResetError, BrokenPipeError):
            pass

    def assert_broker_alive(self):
        """The broker must still accept a fresh, well-formed client"""
        with self.connection() as conn:
            conn.settimeout(2)
            conn.sendall(sol_test.create_connect('alive-check'))
            packet = conn.recv(100)
            connack, rc = sol_test.read_connack(packet)
            self.assertEqual(connack, 32)
            self.assertEqual(rc, 0)
            self.send_disconnect(conn)

    def test_random_garbage(self):
        self.send_raw(b'\x01\x02\x03\x04\x05\xff\xfe\xfd\x00\x7f')
        self.assert_broker_alive()

    def test_reserved_packet_type_zero(self):
        # Type 0 is reserved in MQTT v3.1.1 and has no handler, even after
        # a successful handshake
        conn = self.get_connection()
        conn.settimeout(2)
        try:
            conn.sendall(sol_test.create_connect('type0'))
            conn.recv(100)
            conn.sendall(struct.pack('!BB', 0x00, 0x00))
            self.drain(conn)
        finally:
            conn.close()
        self.assert_broker_alive()

    def test_pingresp_from_client(self):
        # PINGRESP (13) is a server to client packet only
        conn = self.get_connection()
        conn.settimeout(2)
        try:
            conn.sendall(sol_test.create_connect('pingresp'))
            conn.recv(100)
            conn.sendall(struct.pack('!BB', 0xD0, 0x00))
            self.drain(conn)
        finally:
            conn.close()
        self.assert_broker_alive()

    def test_subscribe_before_connect(self):
        # MQTT v3.1.1 §3.1: the first packet must be a CONNECT, reaching a
        # handler without a session must not take the broker down
        self.send_raw(sol_test.create_subscribe(1, {'a': 0}))
        self.assert_broker_alive()

    def test_publish_before_connect(self):
        # A PUBLISH sent before CONNECT must not sidestep the handshake
        publish = struct.pack('!BB', 0x30, 0x07)
        publish += struct.pack('!H3s', 3, b'abc')
        publish += b'hi'
        self.send_raw(publish)
        self.assert_broker_alive()

    def test_truncated_connect(self):
        # Fixed header promising 12 bytes of variable header, only 5 sent
        self.send_raw(struct.pack('!BB', 0x10, 0x0C) + b'\x00\x04MQTT')
        self.assert_broker_alive()

    def test_bad_protocol_name(self):
        # Patch the protocol name length from 4 to 3: an unsupported
        # protocol must be refused with CONNACK 0x01 (§3.1.2.1) and the
        # connection closed (§3.2.2.3), not crash
        conn = self.get_connection()
        conn.settimeout(2)
        try:
            packet = bytearray(sol_test.create_connect('bad-proto'))
            packet[3] = 0x03  # protocol name length LSB
            conn.sendall(bytes(packet))
            reply = conn.recv(100)
            connack, rc = sol_test.read_connack(reply)
            self.assertEqual(connack, 32)
            self.assertEqual(rc, 1)
        finally:
            conn.close()
        self.assert_broker_alive()

    def test_long_client_id(self):
        # The broker accepts up to 63 characters of client id, anything
        # longer is refused with CONNACK 0x02 (§3.1.3.1) and a close
        conn = self.get_connection()
        conn.settimeout(2)
        try:
            conn.sendall(sol_test.create_connect('x' * 100))
            reply = conn.recv(100)
            connack, rc = sol_test.read_connack(reply)
            self.assertEqual(connack, 32)
            self.assertEqual(rc, 2)
        finally:
            conn.close()
        self.assert_broker_alive()

    def test_remaining_length_five_bytes(self):
        # A Remaining Length field of 5 bytes exceeds the 4 allowed by
        # §2.2.3: the packet is malformed and must be dropped
        self.send_raw(struct.pack('!BBBBB', 0x30, 0x80, 0x80, 0x80, 0x80))
        self.assert_broker_alive()

    def test_remaining_length_over_max_request_size(self):
        # 0x0FFFFFFF (the largest legal encoding) is way past
        # max_request_size: refused before a single payload byte is read
        self.send_raw(struct.pack('!BB', 0x30, 0xFF)
                      + b'\xff\xff\x7f')
        self.assert_broker_alive()

    def test_subscribe_with_invalid_qos(self):
        # QoS 7 does not exist (§3.8.3 allows 0, 1 and 2 only)
        conn = self.get_connection()
        conn.settimeout(2)
        try:
            conn.sendall(sol_test.create_connect('bad-qos'))
            conn.recv(100)
            sub = struct.pack('!BB', 0x82, 0x08)
            sub += struct.pack('!H', 1)          # packet id
            sub += struct.pack('!H3sB', 3, b'abc', 7)
            conn.sendall(sub)
            self.drain(conn)
        finally:
            conn.close()
        self.assert_broker_alive()

    def test_subscribe_missing_qos_byte(self):
        # The topic filter consumes every remaining byte, leaving no room
        # for the trailing QoS octet: the parser used to read one byte past
        # the packet and underflow its remaining-length counter, letting a
        # 7-byte SUBSCRIBE allocate tens of thousands of tuples
        conn = self.get_connection()
        conn.settimeout(3)
        try:
            conn.sendall(sol_test.create_connect('sub-noqos'))
            conn.recv(100)
            # Remaining length 5: pkt_id(2) + topic_len(2) + topic 'a'(1),
            # and no QoS byte after it
            conn.sendall(b'\x82\x05\x00\x01\x00\x01a')
            self.drain(conn)
        finally:
            conn.close()
        self.assert_broker_alive()

    def test_publish_to_wildcard_topic(self):
        # A PUBLISH topic name must not contain wildcards (§3.3.1-2)
        conn = self.get_connection()
        conn.settimeout(2)
        try:
            conn.sendall(sol_test.create_connect('wild-topic'))
            conn.recv(100)
            publish = struct.pack('!BB', 0x30, 0x05)
            publish += struct.pack('!H3s', 3, b'a/#')
            conn.sendall(publish)
            self.drain(conn)
        finally:
            conn.close()
        self.assert_broker_alive()

    def test_double_connect(self):
        # Two CONNECT packets on the same connection (§3.1.4) drop the client
        conn = self.get_connection()
        conn.settimeout(2)
        try:
            conn.sendall(sol_test.create_connect('twice'))
            conn.recv(100)
            conn.sendall(sol_test.create_connect('twice'))
            self.drain(conn)
        finally:
            conn.close()
        self.assert_broker_alive()
