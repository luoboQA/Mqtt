#!/usr/bin/env python3
"""
遗嘱 (Will / LWT) 演示 —— 连接时挂号, 掉线时由 broker 代为发布.

先启动 broker:
    ./sol -c etc/conf/sol.conf -v

运行:
    python3 examples/will_demo.py

MQTT v3.1.1 §3.1.2.5: 只有客户端发出 DISCONNECT 才算"正常关闭", 遗嘱会被丢弃;
其余任何断开(进程崩溃 / 网络断 / keepalive 超时 / 协议错误)broker 都必须发布遗嘱.
本脚本依次演示这几种情况, 观察订阅端收到了什么.
"""
import os
import signal
import subprocess
import sys
import threading
import time
import paho.mqtt.client as mqtt

HOST, PORT = "127.0.0.1", 8883
WILL_TOPIC_CRASH = "will/crashed"
WILL_TOPIC_CLEAN = "will/clean"
WILL_TOPIC_KEEPALIVE = "will/keepalive"
WILL_TOPIC_RETAIN = "will/retained"


def new_client(client_id, **kwargs):
    return mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=client_id,
                       **kwargs)


def victim():
    """被 kill 的那个进程: 连上 broker 挂个遗嘱, 然后一直在线等死"""
    c = new_client("victim-crash")
    c.will_set(WILL_TOPIC_CRASH, "客户端被 kill -9 了", qos=1)
    c.connect(HOST, PORT, keepalive=30)
    c.loop_forever()


def main():
    got = []                      # 观察者收到的所有遗嘱
    received = threading.Event()

    watcher = new_client("will-watcher")
    watcher.on_message = lambda c, u, m: (
        got.append((m.topic, m.payload.decode(), m.retain)),
        received.set())
    watcher.connect(HOST, PORT)
    watcher.loop_start()
    watcher.subscribe("will/#", qos=1)
    time.sleep(0.5)

    # --- 1) 正常断开: 客户端发 DISCONNECT, 遗嘱必须被丢弃 ---
    c = new_client("victim-clean")
    c.will_set(WILL_TOPIC_CLEAN, "不该被发布", qos=1)
    c.connect(HOST, PORT)
    c.loop_start()
    time.sleep(0.5)
    c.disconnect()                # 发 DISCONNECT
    c.loop_stop()
    time.sleep(1.0)
    clean = [m for m in got if m[0] == WILL_TOPIC_CLEAN]
    print(f"1) 正常 DISCONNECT     -> 收到 {clean} => "
          f"{'OK (未发布遗嘱)' if not clean else 'FAIL'}")

    # --- 2) 进程被 kill -9: 没有任何收尾, 遗嘱必须被发布 ---
    victim_proc = subprocess.Popen([sys.executable, os.path.abspath(__file__),
                                    "--victim"],
                                   stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL)
    time.sleep(1.0)               # 等它完成 CONNECT
    victim_proc.send_signal(signal.SIGKILL)
    victim_proc.wait()
    received.wait(3)
    crash = [m for m in got if m[0] == WILL_TOPIC_CRASH]
    print(f"2) 进程被 SIGKILL      -> 收到 {crash} => "
          f"{'OK (发布遗嘱)' if crash else 'FAIL'}")

    # --- 3) keepalive 超时: 连着但不发包也不 PING, broker 主动断开 ---
    #     keepalive=1 表示"1.5 秒内没有任何报文就断开"(见 broker 配置 keepalive)
    c = new_client("victim-keepalive")
    c.will_set(WILL_TOPIC_KEEPALIVE, "keepalive 超时", qos=1)
    c.connect(HOST, PORT, keepalive=1)
    # 不启动 loop: 不发 PINGREQ, 连上之后就不再说话
    received.clear()
    received.wait(5)
    ka = [m for m in got if m[0] == WILL_TOPIC_KEEPALIVE]
    print(f"3) keepalive 超时掉线  -> 收到 {ka} => "
          f"{'OK (发布遗嘱)' if ka else 'FAIL (5s 内没等到)'}")

    # --- 4) 遗嘱 + retain: 会替换该主题上的保留消息, 迟到的订阅者也能拿到 ---
    victim_proc = subprocess.Popen([sys.executable, os.path.abspath(__file__),
                                    "--retained-victim"],
                                   stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL)
    time.sleep(1.0)
    victim_proc.send_signal(signal.SIGKILL)
    victim_proc.wait()
    time.sleep(0.8)
    late_got = []
    late = new_client("will-late")
    late.on_message = lambda c, u, m: late_got.append(
        (m.topic, m.payload.decode(), m.retain))
    late.connect(HOST, PORT)
    late.loop_start()
    late.subscribe(WILL_TOPIC_RETAIN, qos=1)
    time.sleep(1.0)
    print(f"4) 遗嘱带 retain        -> 迟到订阅者收到 {late_got} => "
          f"{'OK (retain=1, 保留消息)' if late_got and late_got[0][2] else 'FAIL'}")

    late.loop_stop(); watcher.loop_stop()


def retained_victim():
    c = new_client("victim-retain")
    c.will_set(WILL_TOPIC_RETAIN, "设备离线 (保留)", qos=1, retain=True)
    c.connect(HOST, PORT, keepalive=30)
    c.loop_forever()


if __name__ == "__main__":
    if "--victim" in sys.argv:
        victim()
    elif "--retained-victim" in sys.argv:
        retained_victim()
    else:
        main()
