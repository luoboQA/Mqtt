import os
import signal
import socket
import subprocess
import time
import unittest
import sol_test

CONF = 'tests/integration/persistence.conf'
DB = '/tmp/sol_persistence_test.db'
HOST, PORT = '127.0.0.1', 18885


class TestPersistence(unittest.TestCase):
    """What the broker must not lose across a restart: the retained messages
    and the persistent sessions, with their subscriptions and the messages
    queued while their client was away. The test runs a broker of its own (a
    different port, a configuration that snapshots every second) because it
    has to stop and start it"""

    def setUp(self):
        if os.path.exists(DB):
            os.remove(DB)
        self.broker = None
        self.start_broker()

    def tearDown(self):
        self.stop_broker()
        if os.path.exists(DB):
            os.remove(DB)

    # -- harness ------------------------------------------------------

    def start_broker(self):
        self.broker = subprocess.Popen(
            f'./sol -a {HOST} -p {PORT} -c {CONF}'.split(),
            stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
            preexec_fn=os.setsid)
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                socket.create_connection((HOST, PORT), timeout=0.5).close()
                return
            except OSError:
                time.sleep(0.05)
        self.fail('the broker did not come up')

    def stop_broker(self):
        if not self.broker:
            return
        os.kill(self.broker.pid, signal.SIGTERM)  # clean shutdown: saves once
        self.broker.wait(timeout=5)
        self.broker = None

    def connect(self, client_id, clean_session=True):
        conn = socket.create_connection((HOST, PORT), timeout=5)
        conn.settimeout(5)
        conn.sendall(sol_test.create_connect(client_id,
                                             clean_session=clean_session))
        buf = b''
        while len(buf) < 4:
            buf += conn.recv(100)
        return conn, buf

    def subscribe(self, conn, mid, topics):
        conn.sendall(sol_test.create_subscribe(mid, topics))
        conn.settimeout(3)
        buf = b''
        while len(buf) < 2 or len(buf) < 2 + buf[1]:
            buf += conn.recv(2048)
        code, ack_mid, _ = sol_test.read_suback(buf[:2 + buf[1]])
        self.assertEqual(code, 0x90)
        self.assertEqual(ack_mid, mid)
        return buf[2 + buf[1]:]

    def read_publish(self, conn, buf=b''):
        """Next PUBLISH of the stream, carrying over what was read already:
        the broker may pack several packets in one segment"""
        conn.settimeout(5)
        while True:
            body, remaining = sol_test.mqtt_decode_len(buf)
            # What mqtt_decode_len() leaves in `body` is everything past the
            # fixed header byte and the length field, so what it consumed is
            # already counted there
            total = (len(buf) - len(body)) + remaining
            if len(buf) >= total:
                return sol_test.read_publish(buf[:total]), buf[total:]
            chunk = conn.recv(2048)
            self.assertTrue(chunk, 'the broker closed while a message was due')
            buf += chunk

    # -- tests --------------------------------------------------------

    def test_state_survives_a_restart(self):
        """A retained message, the subscriptions of a persistent session and
        the messages queued for it while it was away all come back after the
        broker is stopped and started again"""
        # A persistent session subscribes and goes away
        sub, _ = self.connect('persist-sub', clean_session=False)
        self.subscribe(sub, 1, {'persist/topic': 1})
        sub.close()
        time.sleep(0.2)

        # One retained message, and two messages for the away session
        pub, _ = self.connect('persist-pub')
        pub.sendall(sol_test.create_publish('persist/retained', b'kept',
                                            qos=1, mid=1, retain=True))
        self.assertEqual(sol_test.read_ack(pub.recv(100))[0], 0x40)
        for mid, payload in ((2, b'first'), (3, b'second')):
            pub.sendall(sol_test.create_publish('persist/topic', payload,
                                                qos=1, mid=mid))
            self.assertEqual(sol_test.read_ack(pub.recv(100))[0], 0x40)
        pub.close()

        # Let a periodic snapshot happen, then restart
        time.sleep(1.5)
        self.stop_broker()
        self.assertTrue(os.path.exists(DB), 'no snapshot was written')
        with open(DB, 'rb') as fh:
            content = fh.read()
        self.assertIn(b'# sol persistence v1', content)
        self.assertIn(b'retained ', content)
        self.assertIn(b'session ', content)
        self.assertIn(b'queued ', content)
        self.start_broker()

        # The retained message is there for a new subscriber
        late, _ = self.connect('persist-late')
        try:
            rest = self.subscribe(late, 1, {'persist/retained': 0})
            publish, _ = self.read_publish(late, rest)
            _, topic, payload, _ = publish
            self.assertEqual(topic, 'persist/retained')
            self.assertEqual(payload, b'kept')
        finally:
            late.close()

        # The persistent session comes back with its subscription and the
        # two messages that were queued for it, in publication order
        again, leftover = self.connect('persist-sub', clean_session=False)
        try:
            connack, rc = sol_test.read_connack(leftover[:4])
            self.assertEqual(connack, 32)
            self.assertEqual(rc, 0)
            # Session present is a connect acknowledge flag (CONNACK byte 3),
            # not a return code: the session rebuilt from disk must show up
            self.assertTrue(leftover[2] & 0x01,
                            'the restored session was not reported present')

            first, leftover = self.read_publish(again, leftover[4:])
            second, _ = self.read_publish(again, leftover)
            self.assertEqual(first[2], b'first')
            self.assertEqual(second[2], b'second')
            self.assertEqual(first[3], 1)  # the identifier replay
        finally:
            again.close()

    def test_empty_persistence_file_is_not_fatal(self):
        """A configuration pointing at a file that does not exist (or holds
        garbage) must start an empty broker, not crash it"""
        self.stop_broker()
        with open(DB, 'wb') as fh:
            fh.write(b'not a record at all\nretained zz zz\n')
        self.start_broker()
        conn = socket.create_connection((HOST, PORT), timeout=5)
        conn.settimeout(3)
        try:
            conn.sendall(sol_test.create_connect('after-garbage'))
            packet = conn.recv(100)
            connack, rc = sol_test.read_connack(packet)
            self.assertEqual(connack, 32)
            self.assertEqual(rc, 0)
        finally:
            conn.close()


if __name__ == '__main__':
    unittest.main()
