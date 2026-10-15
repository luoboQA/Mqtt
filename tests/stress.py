#!/usr/bin/env python3
"""
A small load generator for the broker: N clients, each subscribing to a
shared topic and publishing M messages on it, so every message is delivered
to every subscriber. Meant to be run by hand (it is not part of the
integration suite, which should stay fast):

    python3 tests/stress.py                      # 100 clients, 10 messages each
    python3 tests/stress.py -n 300 -m 20         # heavier
    python3 tests/stress.py --payload 300000     # close to max_request_size
    python3 tests/stress.py --slow               # subscribers that never read

It reports what it managed to do and, at the end, checks that the broker is
still serving clients. `--slow` deliberately leaves the subscribers' sockets
full: the broker has to drop what it cannot buffer (or keep it for a QoS > 0
retransmission) instead of growing without bound or blocking.
"""
import argparse
import os
import signal
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                'integration'))
import sol_test  # noqa: E402

TOPIC = 'bench/topic'


def broker_rss_kb(pid):
    try:
        with open('/proc/%d/status' % pid) as fh:
            for line in fh:
                if line.startswith('VmRSS:'):
                    return int(line.split()[1])
    except OSError:
        pass
    return 0


def connect(port, client_id):
    conn = socket.create_connection(('127.0.0.1', port), timeout=10)
    conn.settimeout(10)
    conn.sendall(sol_test.create_connect(client_id))
    packet = conn.recv(100)
    if not packet or packet[0] != 0x20:
        raise AssertionError('no CONNACK for %s' % client_id)
    return conn


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-n', '--connections', type=int, default=100)
    parser.add_argument('-m', '--messages', type=int, default=10)
    parser.add_argument('-p', '--port', type=int, default=18891)
    parser.add_argument('--payload', type=int, default=64,
                        help='payload size in bytes')
    parser.add_argument('--slow', action='store_true',
                        help='subscribers never read, the broker has to cope')
    parser.add_argument('--qos', type=int, default=0, choices=[0, 1])
    args = parser.parse_args()

    broker = subprocess.Popen(
        f'./sol -a 127.0.0.1 -p {args.port} -c tests/integration/'
        f'sol_test.conf'.split(),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        preexec_fn=os.setsid)
    time.sleep(0.5)

    payload = b'x' * args.payload
    subs, pubs = [], []
    peak_rss = 0
    started = time.time()

    try:
        # Every client subscribes to the shared topic, half of them then
        # publish on it: each message is fanned out to every subscriber
        for i in range(args.connections):
            conn = connect(args.port, 'bench-sub-%d' % i)
            conn.sendall(sol_test.create_subscribe(1, {TOPIC: args.qos}))
            sol_test.read_suback(conn.recv(100))
            subs.append(conn)
        print('%d subscribers connected (%.1fs)'
              % (len(subs), time.time() - started))

        for i in range(max(1, args.connections // 2)):
            pubs.append(connect(args.port, 'bench-pub-%d' % i))

        sent = 0
        for mid in range(1, args.messages + 1):
            for i, conn in enumerate(pubs):
                conn.sendall(sol_test.create_publish(TOPIC, payload,
                                                     qos=args.qos, mid=mid))
                sent += 1
                if args.qos > 0:
                    conn.recv(100)  # PUBACK
            peak_rss = max(peak_rss, broker_rss_kb(broker.pid))

        elapsed = time.time() - started
        print('%d publishers, %d messages sent of %d bytes in %.1fs '
              '(%.0f msg/s)' % (len(pubs), sent, args.payload, elapsed,
                                sent / max(elapsed, 0.001)))

        # Let the delivery settle, then count what the subscribers got
        if not args.slow:
            time.sleep(1.0)
            received = 0
            for conn in subs:
                conn.settimeout(1)
                try:
                    while True:
                        data = conn.recv(65536)
                        if not data:
                            break
                        received += 1
                except socket.timeout:
                    pass
            print('%d subscribers drained (%d reads)' % (len(subs), received))
        else:
            time.sleep(2.0)
            print('slow consumers: the subscribers never read their sockets')

        peak_rss = max(peak_rss, broker_rss_kb(broker.pid))
        print('broker peak RSS: %d KiB' % peak_rss)

        # The broker has to still be there, serving new clients
        check = connect(args.port, 'bench-alive')
        check.close()
        print('broker alive: OK')
    finally:
        for conn in subs + pubs:
            conn.close()
        os.kill(broker.pid, signal.SIGTERM)
        broker.wait(timeout=10)


if __name__ == '__main__':
    main()
