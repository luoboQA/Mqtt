import os
import signal
import socket
import subprocess
import time
import unittest
import sol_test

CONF = 'tests/integration/persistence.conf'
# Same broker, but the periodic snapshot is effectively disabled: everything
# a crash has to replay can only come from the write-ahead log
WAL_CONF = 'tests/integration/persistence_wal.conf'
DB = '/tmp/sol_persistence_test.db'
HOST, PORT = '127.0.0.1', 18885


class TestPersistence(unittest.TestCase):
    """What the broker must not lose across a restart: the retained messages
    and the persistent sessions, with their subscriptions and the messages
    queued while their client was away. The test runs a broker of its own (a
    different port, a configuration that snapshots every second) because it
    has to stop and start it"""

    def setUp(self):
        self.conf = CONF
        self.remove_files()
        self.broker = None
        self.start_broker()

    def tearDown(self):
        self.stop_broker()
        self.remove_files()

    # -- harness ------------------------------------------------------

    def remove_files(self):
        for path in (DB, DB + '.wal'):
            if os.path.exists(path):
                os.remove(path)

    def start_broker(self):
        self.broker = subprocess.Popen(
            f'./sol -a {HOST} -p {PORT} -c {self.conf}'.split(),
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
            while len(buf) < 2:
                chunk = conn.recv(2048)
                self.assertTrue(chunk, 'the broker closed while a message '
                                'was due')
                buf += chunk
            # Walk the Remaining Length field, reading more if it is split
            mult, remaining, pos = 1, 0, 1
            while True:
                if len(buf) <= pos:
                    buf += conn.recv(2048)
                    continue
                byte = buf[pos]
                remaining += (byte & 127) * mult
                mult *= 128
                pos += 1
                if not byte & 128:
                    break
            total = pos + remaining
            if len(buf) >= total:
                return sol_test.read_publish(buf[:total]), buf[total:]
            chunk = conn.recv(2048)
            self.assertTrue(chunk, 'the broker closed while a message was '
                            'due')
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
        # v2 names the log generation and the number of log records it
        # already contains, which is what makes a crash window replay safe
        self.assertRegex(content, rb'# sol persistence v2 \d+ \d+')
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

    def test_inflight_message_survives_a_restart(self):
        """A QoS 1 PUBLISH the subscriber received but never acknowledged is
        in flight state, not offline queue state: a restart must bring it
        back and retransmit it with the DUP flag set once the client
        reconnects, with the same packet identifier"""
        sub, _ = self.connect('persist-inflight', clean_session=False)
        self.subscribe(sub, 1, {'inflight/topic': 1})
        pub, _ = self.connect('inflight-pub')
        try:
            pub.sendall(sol_test.create_publish('inflight/topic', b'pending',
                                                qos=1, mid=1))
            self.assertEqual(sol_test.read_ack(pub.recv(100))[0], 0x40)
        finally:
            pub.close()
        # The delivery reaches the subscriber, which withholds its PUBACK
        publish, _ = self.read_publish(sub)
        _, topic, payload, mid = publish
        self.assertEqual(topic, 'inflight/topic')
        self.assertEqual(payload, b'pending')
        # The persistent session outlives the connection: closing it now
        # keeps the in-flight entry and stops the retransmission scan from
        # resending before the snapshot
        sub.close()

        # Snapshot while the message is still in flight, then restart
        time.sleep(1.5)
        self.stop_broker()
        with open(DB, 'rb') as fh:
            content = fh.read()
        self.assertIn(b'inflight ', content)
        self.start_broker()

        again, leftover = self.connect('persist-inflight', clean_session=False)
        try:
            connack, rc = sol_test.read_connack(leftover[:4])
            self.assertEqual(connack, 32)
            self.assertEqual(rc, 0)
            self.assertTrue(leftover[2] & 0x01,
                            'the restored session was not reported present')
            publish, _ = self.read_publish(again, leftover[4:])
            header, topic, payload, retransmitted = publish
            self.assertEqual(topic, 'inflight/topic')
            self.assertEqual(payload, b'pending')
            self.assertEqual(retransmitted, mid)
            self.assertNotEqual(header & 0x08, 0, 'the retransmission was '
                                'not flagged as a duplicate')
            again.sendall(sol_test.create_puback(retransmitted))
        finally:
            again.close()

    def test_wal_replays_mutations_lost_by_a_crash(self):
        """With the periodic snapshot disabled every mutation lives only in
        the write-ahead log: SIGKILL leaves no snapshot behind, and the next
        start must rebuild the retained message, the session, its
        subscription and the messages queued for it from the log alone"""
        self.stop_broker()
        self.conf = WAL_CONF
        self.remove_files()
        self.start_broker()

        sub, _ = self.connect('wal-sub', clean_session=False)
        self.subscribe(sub, 1, {'wal/topic': 1})
        sub.close()
        time.sleep(0.2)

        pub, _ = self.connect('wal-pub')
        pub.sendall(sol_test.create_publish('wal/retained', b'from-wal',
                                            qos=1, mid=1, retain=True))
        self.assertEqual(sol_test.read_ack(pub.recv(100))[0], 0x40)
        for mid, payload in ((2, b'first'), (3, b'second')):
            pub.sendall(sol_test.create_publish('wal/topic', payload,
                                                qos=1, mid=mid))
            self.assertEqual(sol_test.read_ack(pub.recv(100))[0], 0x40)
        pub.close()

        # Crash: no clean shutdown, therefore no snapshot
        os.kill(self.broker.pid, signal.SIGKILL)
        self.broker.wait(timeout=5)
        self.broker = None
        self.assertFalse(os.path.exists(DB), 'a snapshot was written anyway')
        self.assertTrue(os.path.exists(DB + '.wal'), 'no log was written')

        self.start_broker()

        # The retained message came back from the log
        late, _ = self.connect('wal-late')
        try:
            rest = self.subscribe(late, 1, {'wal/retained': 0})
            publish, _ = self.read_publish(late, rest)
            _, topic, payload, _ = publish
            self.assertEqual(topic, 'wal/retained')
            self.assertEqual(payload, b'from-wal')
        finally:
            late.close()

        # So did the persistent session, its subscription and its queue
        again, leftover = self.connect('wal-sub', clean_session=False)
        try:
            connack, rc = sol_test.read_connack(leftover[:4])
            self.assertEqual(rc, 0)
            self.assertTrue(leftover[2] & 0x01)
            first, leftover = self.read_publish(again, leftover[4:])
            second, _ = self.read_publish(again, leftover)
            self.assertEqual(first[2], b'first')
            self.assertEqual(second[2], b'second')
        finally:
            again.close()

    def test_log_records_already_in_the_snapshot_are_not_replayed(self):
        """A crash between the snapshot rename and the log truncation leaves
        a log whose records are already contained in the snapshot. The header
        names how many: they must be skipped, or the reconnect would see a
        queue the file had already flushed"""
        # The running broker would overwrite the crafted files on shutdown
        self.stop_broker()
        sess = b'skip-sess'
        topic = b'skip/topic'

        def h(data):
            return data.hex()

        lines = [b'# sol persistence v2 5 1',
                 b'session ' + h(sess).encode(),
                 b'sub ' + h(sess).encode() + b' ' + h(topic).encode()
                 + b' 1 0']
        for mid, payload in ((1, b'first'), (2, b'second'), (3, b'third')):
            lines.append(b'queued ' + h(sess).encode() + b' '
                         + h(topic).encode() + b' 1 %d ' % mid
                         + h(payload).encode())
        with open(DB, 'wb') as fh:
            fh.write(b'\n'.join(lines) + b'\n')

        # The same log is what the snapshot was taken from, plus the fourth
        # message that arrived after it
        wal = [b'# sol wal v2 5',
               b'flush ' + h(sess).encode() + b' 3',
               b'queued ' + h(sess).encode() + b' ' + h(topic).encode()
               + b' 1 4 ' + h(b'fourth').encode()]
        with open(DB + '.wal', 'wb') as fh:
            fh.write(b'\n'.join(wal) + b'\n')

        self.start_broker()

        again, leftover = self.connect('skip-sess', clean_session=False)
        try:
            connack, rc = sol_test.read_connack(leftover[:4])
            self.assertEqual(rc, 0)
            self.assertTrue(leftover[2] & 0x01)
            got = []
            buf = leftover[4:]
            while len(got) < 4:
                publish, buf = self.read_publish(again, buf)
                got.append(publish[2])
            # The three snapshot messages are still there: the logged flush
            # was already applied to the snapshot and was skipped
            self.assertEqual(got, [b'first', b'second', b'third',
                                   b'fourth'])
        finally:
            again.close()

    def test_pubrel_state_survives_a_restart(self):
        """The PUBREL half of a QoS 2 delivery (the PUBLISH was acknowledged,
        the PUBCOMP is still outstanding) is in flight state too: a restart
        must resend the PUBREL on reconnect, not forget the flow"""
        sub, _ = self.connect('persist-pubrel', clean_session=False)
        self.subscribe(sub, 1, {'pubrel/topic': 2})
        pub, _ = self.connect('pubrel-pub')
        try:
            pub.sendall(sol_test.create_publish('pubrel/topic', b'rel',
                                                qos=2, mid=1))
            self.assertEqual(pub.recv(100)[0], 0x50)  # PUBREC
            pub.sendall(sol_test.create_pubrel(1))
            self.assertEqual(pub.recv(100)[0], 0x70)  # PUBCOMP
        finally:
            pub.close()

        publish, _ = self.read_publish(sub)
        header, topic, payload, mid = publish
        self.assertEqual(payload, b'rel')
        sub.sendall(sol_test.create_pubrec(mid))
        # The PUBREL arrives; the PUBCOMP is withheld, leaving the flow open
        self.assertEqual(sub.recv(100)[0], 0x62)
        sub.close()

        time.sleep(1.5)
        self.stop_broker()
        with open(DB, 'rb') as fh:
            content = fh.read()
        self.assertIn(b'ack ', content)
        self.start_broker()

        again, leftover = self.connect('persist-pubrel', clean_session=False)
        try:
            connack, rc = sol_test.read_connack(leftover[:4])
            self.assertEqual(rc, 0)
            self.assertTrue(leftover[2] & 0x01)
            # The restored entry is due immediately: the periodic scan
            # resends the PUBREL with the same identifier
            buf = leftover[4:]
            again.settimeout(5)
            while len(buf) < 4:
                chunk = again.recv(100)
                self.assertTrue(chunk, 'the broker closed instead of '
                                'resending the PUBREL')
                buf += chunk
            self.assertEqual(buf[0], 0x62)
            self.assertEqual(buf[2:4], bytes([mid >> 8, mid & 0xFF]))
            again.sendall(sol_test.create_pubcomp(mid))
        finally:
            again.close()

    def test_inbound_qos2_flow_survives_a_restart(self):
        """The inbound QoS 2 deduplication set is session state: a publisher
        that retransmits a PUBLISH whose PUBREL never reached the broker
        before a restart must still be recognised as a duplicate, and the
        message must not be delivered twice"""
        sub, _ = self.connect('persist-q2sub', clean_session=False)
        self.subscribe(sub, 1, {'q2flow/topic': 2})
        pub, _ = self.connect('persist-q2pub', clean_session=False)
        try:
            pub.sendall(sol_test.create_publish('q2flow/topic', b'once',
                                                qos=2, mid=7))
            self.assertEqual(pub.recv(100)[0], 0x50)  # PUBREC
            # Complete the subscriber side, then leave the publisher's flow
            # open by withholding its PUBREL
            publish, _ = self.read_publish(sub)
            _, topic, payload, mid = publish
            self.assertEqual(payload, b'once')
            sub.sendall(sol_test.create_pubrec(mid))
            self.assertEqual(sub.recv(100)[0], 0x62)  # PUBREL
            sub.sendall(sol_test.create_pubcomp(mid))
        finally:
            pub.close()
        sub.close()

        time.sleep(1.5)
        self.stop_broker()
        with open(DB, 'rb') as fh:
            content = fh.read()
        self.assertIn(b'q2 ', content)
        self.start_broker()

        again, leftover = self.connect('persist-q2pub', clean_session=False)
        try:
            connack, rc = sol_test.read_connack(leftover[:4])
            self.assertEqual(rc, 0)
            self.assertTrue(leftover[2] & 0x01)
            # The retransmission of the very same PUBLISH is a duplicate:
            # PUBREC only, no delivery
            again.sendall(sol_test.create_publish('q2flow/topic', b'once',
                                                  qos=2, mid=7))
            buf = leftover[4:]
            again.settimeout(5)
            while len(buf) < 4:
                buf += again.recv(100)
            self.assertEqual(buf[0], 0x50)
            self.assertEqual(buf[2:4], b'\x00\x07')
            again.sendall(sol_test.create_pubrel(7))
            buf = buf[4:]
            while len(buf) < 4:
                buf += again.recv(100)
            self.assertEqual(buf[0], 0x70)  # PUBCOMP
        finally:
            again.close()

        # Nothing was queued for the subscriber by the duplicate
        sub, leftover = self.connect('persist-q2sub', clean_session=False)
        try:
            connack, rc = sol_test.read_connack(leftover[:4])
            self.assertEqual(rc, 0)
            self.assertTrue(leftover[2] & 0x01)
            self.assertEqual(leftover[4:], b'',
                             'the duplicate was delivered after the restart')
            sub.settimeout(2)
            with self.assertRaises(socket.timeout):
                sub.recv(100)
        finally:
            sub.close()

    def test_newer_log_generation_is_replayed_from_the_start(self):
        """The normal crash window: a snapshot of generation N is on disk and
        the log has already moved to generation N+1 (the previous compaction
        bumped it). Every record of that log is newer than the snapshot and
        must be replayed, with no skip count applied"""
        self.stop_broker()
        sess = b'gen-sess'
        topic = b'gen/topic'

        def h(data):
            return data.hex()

        with open(DB, 'wb') as fh:
            fh.write(b'# sol persistence v2 1 0\n'
                     b'session ' + h(sess).encode() + b'\n'
                     b'sub ' + h(sess).encode() + b' ' + h(topic).encode()
                     + b' 1 0\n'
                     b'queued ' + h(sess).encode() + b' '
                     + h(topic).encode() + b' 1 1 ' + h(b'first').encode()
                     + b'\n')
        with open(DB + '.wal', 'wb') as fh:
            fh.write(b'# sol wal v2 2\n'
                     b'queued ' + h(sess).encode() + b' '
                     + h(topic).encode() + b' 1 2 ' + h(b'second').encode()
                     + b'\n')
        self.start_broker()

        again, leftover = self.connect('gen-sess', clean_session=False)
        try:
            connack, rc = sol_test.read_connack(leftover[:4])
            self.assertEqual(rc, 0)
            self.assertTrue(leftover[2] & 0x01)
            first, leftover = self.read_publish(again, leftover[4:])
            second, _ = self.read_publish(again, leftover)
            self.assertEqual(first[2], b'first')
            self.assertEqual(second[2], b'second')
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
