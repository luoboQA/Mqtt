import os
import time
import struct
import signal
import subprocess


def start_broker(host='127.0.0.1', port=1883):
    proc = subprocess.Popen(
        f'./sol -a {host} -p {port} '
        f'-c tests/integration/sol_test.conf'.split(),
        stdout=subprocess.PIPE,
        preexec_fn=os.setsid
    )
    time.sleep(.5)
    return proc


def kill_broker(proc):
    os.kill(proc.pid, signal.SIGTERM)


def mqtt_encode_len(remaining_length):
    s = b""
    while remaining_length:
        byte = remaining_length % 128
        remaining_length //= 128
        # If there are more digits to encode, set the top bit of this digit
        if remaining_length > 0:
            byte |= 0x80

        s += struct.pack("!B", byte)
    return s


def mqtt_decode_len(packet):
    pk_len = min(5, len(packet))
    all_bytes = struct.unpack("!" + "B" * pk_len, packet[:pk_len])
    mult = 1
    rl = 0
    for i in range(1, pk_len - 1):
        byte = all_bytes[i]

        rl += (byte & 127) * mult
        mult *= 128
        if byte & 128 == 0:
            packet = packet[i + 1:]
            break

    return packet, rl


def create_connect(client_id=None, clean_session=True, keepalive=60,
                   username=None, password=None, will_topic=None, will_qos=0,
                   will_retain=False, will_payload=b""):
    remaining_length = 10
    if client_id is not None:
        client_id = client_id.encode("utf-8")
        remaining_length += 2 + len(client_id)
    else:
        remaining_length += 2

    flags = 0

    if clean_session:
        flags |= 0x02

    if will_topic is not None:
        will_topic = will_topic.encode("utf-8")
        remaining_length += 2 + len(will_topic) + 2 + len(will_payload)
        flags |= 0x04 | ((will_qos & 0x03) << 3)
        if will_retain:
            flags |= 32

    if username is not None:
        username = username.encode("utf-8")
        remaining_length += 2 + len(username)
        flags |= 0x80
        if password is not None:
            password = password.encode("utf-8")
            flags |= 0x40
            remaining_length += 2 + len(password)

    packet_len = mqtt_encode_len(remaining_length)
    packet = struct.pack("!B" + str(len(packet_len)) + "s", 0x10, packet_len)
    packet += struct.pack("!H4sBBH", len(b"MQTT"), b"MQTT", 4, flags, keepalive)

    if client_id != None:
        packet += struct.pack("!H" + str(len(client_id)) + "s", len(client_id), bytes(client_id))
    else:
        packet += struct.pack("!H", 0)

    if will_topic is not None:
        packet += struct.pack("!H" + str(len(will_topic)) + "s", len(will_topic), will_topic)
        if len(will_payload) > 0:
            packet += struct.pack("!H" + str(len(will_payload)) + "s", len(will_payload), will_payload)
        else:
            packet += struct.pack("!H", 0)

    if username != None:
        packet += struct.pack("!H" + str(len(username)) + "s", len(username), username)
        if password is not None:
            packet += struct.pack("!H" + str(len(password)) + "s", len(password), password)
    return packet


def create_disconnect(rc=0):
    return struct.pack('!BBB', 0xE0, 1, rc)


def read_connack(packet):
    # The CONNACK can share a single TCP segment with whatever follows it
    # (e.g. the queued messages flushed on a resumed session): only its
    # first four bytes belong to the packet itself
    cmd, _, _, rc = struct.unpack('!BBBB', packet[:4])
    return cmd, rc


def read_ack(packet):
    code, plen = struct.unpack('!BH', packet[:3])
    return code, struct.unpack('!B', packet[3:])[0]


def create_subscribe(mid, topics):
    packet = struct.pack("!B", 0x80)
    for topic, qos in topics.items():
        topic = topic.encode("utf-8")
        packet += mqtt_encode_len(2 + 2 + len(topic) + 1)
        pack_format = "!HH" + str(len(topic)) + "sB"
        packet += struct.pack(pack_format, mid, len(topic), topic, qos)
    return packet


def create_unsubscribe(mid, topic):
    topic = topic.encode("utf-8")
    pack_format = "!BBHH" + str(len(topic)) + "s"
    return struct.pack(pack_format, 0xA0, 2 + 2 + len(topic), mid, len(topic), topic)


def read_suback(packet):
    header = packet[0]
    packet, plen = mqtt_decode_len(packet)
    pack_format = "!H" + str(len(packet) - 2) + 's'
    mid, packet = struct.unpack(pack_format, packet)
    pack_format = "!" + "B" * len(packet)
    granted_qos = struct.unpack(pack_format, packet)
    return header, mid, granted_qos[0]


def create_publish(topic, payload, qos=0, mid=1, retain=False):
    topic = topic.encode("utf-8") if isinstance(topic, str) else topic
    payload = payload.encode("utf-8") if isinstance(payload, str) else payload
    header = 0x30 | ((qos & 0x03) << 1) | (1 if retain else 0)
    remaining = 2 + len(topic) + len(payload) + (2 if qos > 0 else 0)
    packet = struct.pack("!B", header) + mqtt_encode_len(remaining)
    packet += struct.pack("!H", len(topic)) + topic
    if qos > 0:
        packet += struct.pack("!H", mid)
    packet += payload
    return packet


def read_publish(packet):
    """Decode the first PUBLISH of a buffer: (header, topic, payload, mid)"""
    header = packet[0]
    qos = (header >> 1) & 0x03
    body, _ = mqtt_decode_len(packet)
    tlen = struct.unpack('!H', body[:2])[0]
    topic = body[2:2 + tlen].decode('utf-8')
    pos = 2 + tlen
    mid = None
    if qos > 0:
        mid = struct.unpack('!H', body[pos:pos + 2])[0]
        pos += 2
    return header, topic, body[pos:], mid


def create_ack(header, mid):
    """PUBACK/PUBREC/PUBREL/PUBCOMP share the same 4 bytes shape, only the
    fixed header byte changes"""
    return struct.pack("!BBH", header, 2, mid)


def create_puback(mid):
    return create_ack(0x40, mid)


def create_pubrec(mid):
    return create_ack(0x50, mid)


def create_pubrel(mid):
    return create_ack(0x62, mid)


def create_pubcomp(mid):
    return create_ack(0x70, mid)
