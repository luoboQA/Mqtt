#!/usr/bin/env python3
"""
$SOL 统计主题 + '$' 前缀规则演示.

先启动 broker:
    ./build/sol -c etc/conf/sol.conf -v

运行:
    python3 examples/stats_demo.py

broker 每隔 stats_publish_interval (etc/conf/sol.conf 里是 10s) 秒把自身状态发布到
$SOL/broker/... 上. 本脚本同时开两个订阅者对比:

  订阅 $SOL/#  -> 收到统计
  订阅 #       -> 收不到统计

因为 MQTT v3.1.1 §4.7.2 规定: 通配符开头的过滤器不匹配以 '$' 开头的主题名,
必须显式写出 '$' 才能订阅. 这就是 mosquitto 里 $SYS、本 broker 里 $SOL 的保护机制.

想跑快一点可以把 etc/conf/sol.conf 的 stats_publish_interval 改成 2s.
"""
import threading
import time
import paho.mqtt.client as mqtt

HOST, PORT = "127.0.0.1", 8883
STATS_WAIT = 13          # 等一轮统计发布, 配置是 10s 间隔, 多留几秒余量


def new_client(client_id):
    return mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=client_id)


def main():
    stats = {}                     # $SOL/# 订阅者看到的统计 (同一主题只留最新值)
    stats_ready = threading.Event()
    star_got = []                  # 裸 '#' 订阅者看到的一切

    stats_sub = new_client("stats-sub")
    stats_sub.on_message = lambda c, u, m: (
        stats.__setitem__(m.topic, m.payload.decode()), stats_ready.set())
    stats_sub.connect(HOST, PORT)
    stats_sub.loop_start()
    stats_sub.subscribe("$SOL/#", qos=0)

    star_sub = new_client("stats-star")
    star_sub.on_message = lambda c, u, m: star_got.append(m.topic)
    star_sub.connect(HOST, PORT)
    star_sub.loop_start()
    star_sub.subscribe("#", qos=0)
    time.sleep(0.5)

    print(f"1) 等一轮统计 (最多 {STATS_WAIT}s)...")
    stats_ready.wait(STATS_WAIT)
    time.sleep(1.0)                # 同一批统计可能分几条到, 再收一会儿
    for topic in sorted(stats):
        print(f"   {topic:<40} = {stats[topic]}")
    print(f"   => {'OK' if stats else 'FAIL (没收到任何 $SOL 统计)'}")

    # 裸 '#' 应该一条 $SOL 都收不到 (但它们确实在线, 见第 3 步)
    leaked = [t for t in star_got if t.startswith("$")]
    print(f"2) 裸 '#' 订阅者收到 $SOL 数量: {len(leaked)} {leaked} => "
          f"{'OK (通配符不匹配 $ 开头主题)' if not leaked else 'FAIL'}")

    # 普通主题两个都该收到 (证明 '#' 订阅者是活的, 第 2 步的 0 不是因为它掉线)
    before = len(star_got)
    pub = new_client("stats-pub")
    pub.connect(HOST, PORT)
    pub.loop_start()
    pub.publish("demo/hello", "hi", qos=0)
    time.sleep(1.0)
    star_ok = len(star_got) > before and "demo/hello" in star_got
    stats_ok = "demo/hello" not in stats
    print(f"3) 发布 demo/hello: 裸 '#' 收到={star_ok}, $SOL/# 收到={not stats_ok} => "
          f"{'OK' if star_ok and stats_ok else 'FAIL'}")

    pub.loop_stop(); star_sub.loop_stop(); stats_sub.loop_stop()


if __name__ == "__main__":
    main()
