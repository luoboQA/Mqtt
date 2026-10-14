import socket
import unittest
import sol_test
import base_testcase


class TestConnect(base_testcase.BaseTestcase):

    def test_connect(self):
        with self.connection() as conn:
            connect_packet = sol_test.create_connect()
            conn.send(connect_packet)
            packet = conn.recv(100)
            connack, rc = sol_test.read_connack(packet)
            self.assertEqual(connack, 32)
            self.assertEqual(rc, 0)
            self.send_disconnect(conn)

    def test_connect_with_client_id(self):
        with self.connection() as conn:
            connect_packet = sol_test.create_connect('test-user')
            conn.send(connect_packet)
            packet = conn.recv(100)
            connack, rc = sol_test.read_connack(packet)
            self.assertEqual(connack, 32)
            self.assertEqual(rc, 0)
            self.send_disconnect(conn)

    def test_connect_wrong_protocol_level(self):
        # MQTT v3.1.1 is level 4: anything else must be refused with a
        # CONNACK carrying return code 0x01 (§3.1.2.1), followed by the
        # broker closing the connection (§3.2.2.3)
        with self.connection() as conn:
            connect_packet = sol_test.create_connect('test-user')
            # Patch the protocol level byte: fixed header (1) + remaining
            # length (1) + protocol name (2 + 4), level follows
            connect_packet = (connect_packet[:8] + bytes([5])
                              + connect_packet[9:])
            conn.send(connect_packet)
            packet = conn.recv(100)
            connack, rc = sol_test.read_connack(packet)
            self.assertEqual(connack, 32)
            self.assertEqual(rc, 1)
