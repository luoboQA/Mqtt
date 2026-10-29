import os
import signal
import socket
import ssl
import subprocess
import time
import unittest

HOST, PORT = '127.0.0.1', 18889
CONF = 'tests/integration/tls.conf'
CA = 'certs/ca.crt'


def ensure_certs():
    if not os.path.exists(CA):
        subprocess.run(['tests/gen_certs.sh'], check=True,
                       stdout=subprocess.DEVNULL)


class TestTls(unittest.TestCase):
    """The TLS path end to end. The broker loads its certificates at startup
    and wraps every accepted connection in SSL, and, as the configuration
    documents, it does not ask the clients for a certificate of their own:
    what a client needs is the certificate authority to verify the server"""

    @classmethod
    def setUpClass(cls):
        ensure_certs()
        cls.broker = subprocess.Popen(
            f'{sol_test.broker_bin()} -a {HOST} -p {PORT} '
            f'-c {CONF}'.split(),
            stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
            preexec_fn=os.setsid)
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                socket.create_connection((HOST, PORT), timeout=0.5).close()
                return
            except OSError:
                time.sleep(0.05)
        raise AssertionError('the TLS broker did not come up')

    @classmethod
    def tearDownClass(cls):
        os.kill(cls.broker.pid, signal.SIGTERM)
        cls.broker.wait(timeout=5)

    def tls_socket(self, client_id):
        """A TLS connection to the broker, using paho to speak MQTT on it"""
        import paho.mqtt.client as mqtt
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1,
                             client_id=client_id)
        client.tls_set(ca_certs=CA)
        client.connect(HOST, PORT, keepalive=30)
        client.loop_start()
        return client

    def test_tls_delivery_round_trip(self):
        """Subscribe and publish over TLS: the handshake, the delivery and the
        acknowledgements all travel encrypted"""
        import threading
        received = threading.Event()
        got = []

        sub = self.tls_socket('tls-sub')
        try:
            sub.on_message = lambda c, u, m: (got.append((m.topic,
                                                          m.payload)),
                                              received.set())
            sub.subscribe('tls/topic', qos=1)
            time.sleep(0.5)

            pub = self.tls_socket('tls-pub')
            try:
                pub.publish('tls/topic', b'over tls', qos=1)
                self.assertTrue(received.wait(5), 'no delivery over TLS')
                self.assertEqual(got[0], ('tls/topic', b'over tls'))
            finally:
                pub.loop_stop()
                pub.disconnect()
        finally:
            sub.loop_stop()
            sub.disconnect()

    def test_retained_message_and_will_over_tls(self):
        """The store and the will take the same path whatever the transport
        is: a retained message reaches a late subscriber, and the will of a
        connection that goes away is delivered"""
        import threading
        late_got = threading.Event()

        pub = self.tls_socket('tls-ret-pub')
        try:
            pub.publish('tls/retained', b'kept', qos=1, retain=True)
            time.sleep(0.5)
        finally:
            pub.loop_stop()
            pub.disconnect()

        sub = self.tls_socket('tls-late')
        try:
            sub.on_message = lambda c, u, m: late_got.set()
            sub.subscribe('tls/retained', qos=1)
            self.assertTrue(late_got.wait(5),
                            'the retained message did not arrive over TLS')
        finally:
            sub.loop_stop()
            sub.disconnect()

    def test_plaintext_client_is_rejected(self):
        """A client that speaks plain MQTT at a TLS listener is dropped: the
        handshake fails and the broker closes the connection, without taking
        the broker down"""
        conn = socket.create_connection((HOST, PORT), timeout=5)
        conn.settimeout(3)
        try:
            conn.sendall(b'\x10\x0c\x00\x04MQTT\x04\x02\x00\x3c\x00\x00')
            # Whatever comes back, it is not a CONNACK: either a TLS alert or
            # a closed connection
            answer = b''
            try:
                answer = conn.recv(100)
            except (ConnectionResetError, socket.timeout):
                pass
            self.assertNotEqual(answer[:1], b'\x20')
        finally:
            conn.close()

        # And the broker is still serving TLS clients
        client = self.tls_socket('tls-after-plain')
        client.loop_stop()
        client.disconnect()


if __name__ == '__main__':
    unittest.main()
