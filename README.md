[![Build Status](https://travis-ci.org/codepr/sol.svg?branch=master)](https://travis-ci.org/codepr/sol)

Sol
===

Oversimplified MQTT broker written from scratch, which mimic mosquitto
features. Implemented to learning how the protocol works, it supports
almost all MQTT v3.1.1 commands on linux platform and relies on EPOLL interface
for multiplexing I/O. The basic development process is documented in this
[series of posts](https://codepr.github.io/posts/sol-mqtt-broker), referring to
the [tutorial branch](https://github.com/codepr/sol/tree/tutorial); the master
branch is the most up-to-date and tested.
**Not for production use**.

                    ┌─────────────────────────────┐
                    │         sol.c (main)        │
                    │  解析命令行 / 加载配置 / 启动   │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │      server.c (核心)         │
                    │  event-loop + 回调 + 客户端池  │
                    └──┬────────┬────────┬─────────┘
                       │        │        │
        ┌──────────────▼─┐ ┌────▼────┐ ┌─▼──────────┐
        │ ev.c (多路复用) │ │network.c│ │handlers.c  │
        │epoll/poll/select││socket/TLS││各MQTT命令   │
        └────────────────┘ └─────────┘ └─┬──────────┘
                                         │
                       ┌─────────────────▼─────────────────┐
                       │  mqtt.c (编解码) + pack.c (字节序)  │
                       └─────────────────┬─────────────────┘
                                         │
        ┌────────────────────────────────▼──────────────────────────┐
        │ 数据结构: trie.c(主题树) bst.c(AVL子节点) list.c iterator.c  │
        │          topic_store.c(通配符索引) memory/memorypool.c      │
        └───────────────────────────────────────────────────────────┘

### Features

It's still a work in progress but it already handles the most of the basic
features expected from an MQTT broker. It does not leak memory as of now,
there's probably some corner cases, not deeply investigated yet.

- Configuration file on disk
- Support for QoS messages 0, 1 and 2
- Retained messages per topic
- `max_memory` enforcement: unused topics are reclaimed, the oldest queued
  messages are dropped under pressure and new connections are refused when
  nothing is left to give back (`reclaim_interval` sets the idle interval)
- Session present check and handling
- Last Will & Testament, delivered on any disconnection that is not a clean
  DISCONNECT (network error, protocol error, keepalive expiry)
- Periodic stats publishing
- Support multiple topics subscriptions through wildcard (#) and (+) for single
  level wildcard e.g. foo/+/bar/#
- Authentication through username and password
- Topic level authorization through an access control file (`acl_file`):
  per client or per user rules of `read`/`write`/`readwrite` over topic
  filters, evaluated in order, deny by default
- SSL/TLS connections, configuration accepts minimum protocols to be used
- Logging on disk
- Daemon mode
- Multiplexing IO with abstraction over backend, currently supports
  select/poll/epoll choosing the better implementation.
- Multithread load-balancing on connections for high concurrent performance

### To be implemented

- Persistence on disk for inflight messages on disconnected clients
- Check on max memory used

### Maybe

- MQTT 5.0 support
- Porting for BSD/MAC osx
- Scaling (feeling brave?)

### 教学简化 / Known simplifications

为了代码好读，broker 有意做了几处简化。做实验时不要把下面这些当成 MQTT 规范：

- **主题名统一补末尾的 `/`**：内部所有主题都归一化成 `foo/bar/`，所以 `a` 和 `a/`
  会被当成同一个主题；按规范它们是两个不同的主题名（后者多一个空层级）。
- **`$` 开头的主题可以发布**：`$SOL/...` 只是命名约定，broker 不拦客户端发布。订阅侧
  已按 §4.7.2 处理：`#`/`+` 开头的过滤器不匹配 `$` 开头的主题，必须显式订阅
  `$SOL/#`（见 `examples/stats_demo.py`）。
- **`max_memory` 是三级策略而不是硬上限**：超限时依次"回收空主题 → 丢弃最旧的离线
  队列消息 → 拒绝新连接"，每一步都会打日志。低于进程基线（约 50MB，主要是客户端
  内存池与每条连接 2×`max_request_size` 的缓冲）的限额会让 broker 一开始就拒收连接，
  启动时会有一条 warning。
- **每个会话固定预分配 65536 项的在途表**（`i_msgs`/`i_acks`，约 2MB/会话），
  会话数量很大时内存增长很快。
- **没有磁盘持久化**：`clean_session=false` 的离线消息只存在于内存里，进程重启即丢。

## Build

```sh
$ cmake .
$ make
```

### Event loop backends

The multiplexing backend is picked from the kernel the broker is built for,
and can be forced at configure time to exercise the other implementations on
the same machine: `cmake -S . -B build-poll -DEVENTLOOP=poll` (or `select`,
`epoll`; Linux only). The running backend is printed at startup.

### Sanitizer builds

```sh
$ cmake -S . -B build-asan -DDEBUG=ON && make -C build-asan sol   # ASan + UBSan
$ cmake -S . -B build-tsan -DTSAN=ON && make -C build-tsan sol   # TSan
```

Both sanitizers are meant to be exercised through the integration suite: run it
with the sanitized binary in place of `./sol`. The ThreadSanitizer one needs
the address space randomization disabled (it refuses to start otherwise on
recent kernels), which `tests/run_tsan.sh` takes care of.

## Quickstart play

The broker can be tested using `mosquitto_sub` and `mosquitto_pub` or with
`paho-mqtt` python driver. There is a set of guided scripts under `examples/`
(connect/subscribe/publish, QoS 0-2, retained messages, wildcards, will
messages, `$SOL` statistics), each one printing what it expects to observe:
start the broker with `./sol -c etc/conf/sol.conf -v` and run them with
`python3 examples/<script>.py` — see `examples/README.md`.

To run the broker with DEBUG logging on:

```sh
$ ./sol -v
```

A simple configuration can be passed in with `-c` flag:

```sh
$ ./sol -c path/to/sol.conf
```

As of now the configuration is very small and it's possible to set some of the
most common parameters, default path is located to `/etc/sol/sol.conf`:

```sh
# Sol configuration file, uncomment and edit desired configuration

# Network configuration

# Comment ip_address and ip_port, set unix_socket and
# UNIX family socket will be used instead

ip_address 127.0.0.1
ip_port 8883

# unix_socket /tmp/sol.sock

# Logging configuration

# Could be either DEBUG, INFO/INFORMATION, WARNING, ERROR
log_level DEBUG

log_path /tmp/sol.log

# Max memory to be used, after which the system starts to reclaim memory by
# freeing older items stored
max_memory 2GB

# Max memory that will be allocated for each request
max_request_size 2MB

# TCP backlog, size of the complete connection queue
tcp_backlog 128

# Interval of time between one stats publish on $SOL topics and the subsequent
stats_publish_interval 10s

# Seconds after which an unacknowledged QoS 1/2 message (or a PUBREL still
# waiting for its PUBCOMP) is retransmitted with the DUP flag
# retransmit_interval 20s

# Upper bound of the Keep Alive clients negotiate: a client asking for a
# longer period, or for none at all (Keep Alive 0), is still held to it, and
# dropped after one and a half times this value of silence. 0 disables the cap
# keepalive 60s

# TLS certs paths, cafile act as a flag as well to set TLS/SSL ON
# cafile /etc/sol/certs/ca.crt
# certfile /etc/sol/certs/cert.crt
# keyfile /etc/sol/certs/cert.key

# Authentication
# allow_anonymous false
# password_file /etc/sol/passwd

# Access control list, one rule per line:
#   <allow|deny> <read|write|readwrite> <client:id|user:name|*> <topic filter>
# evaluated in order, a topic no rule covers is denied
# acl_file /etc/sol/sol.acl

# TLS protocols, supported versions should be listed comma separated
# example:
# tls_protocols tlsv1_2,tlsv1_3
```

If `allow_anonymous` is false, a password file have to be specified. The
password file follows the standard format of username:password line by line.
To generate one just add all entries needed in a file and run passwd.py after
it:

```sh
$ cat sample_passwd_file
user1:pass1
user2:pass2
$ python passwd.py sample_passwd_file
$ cat sample_passwd_file
user1:$6$69qVAELLWuKXWQPQ$oO7lP/hNS4WPABTyK4nkJs4bcRLYFi365YX13cEc/QBJtQgqf2d5rOIUdqoUin.YVGXC3OXY9MSz7Z66ZDkCW/
user2:$6$vtHdafhGhxpXwgBa$Y3Etz8koC1YPSYhXpTnhz.2vJTZvCUGk3xUdjyLr9z9XgE8asNwfYDRLIKN4Apz48KKwKz0YntjHsPRiE6r3g/
```

## Concurrency

The broker provides an access through a simple IO multiplexing event-loop based
TCP server, the model is designed to be self-contained and thus easy to spread
on multiple threads. Another approach and probably more elegant would be to
share a single event loop to multiple threads, at the cost of higher complexity
and race-conditions to be handled.

```
                            THREADS 1..N
                            [EVENT-LOOP]

    ACCEPT_CALLBACK         READ_CALLBACK         WRITE_CALLBACK
  -------------------    ------------------    --------------------
          |                      |                       |
       ACCEPT                    |                       |
          | -------------------> |                       |
          |                READ AND DECODE               |
          |                      |                       |
          |                      |                       |
          |                   PROCESS                    |
          |                      |                       |
          |                      |                       |
          |                      | --------------------> |
          |                      |                     WRITE
       ACCEPT                    |                       |
          | -------------------> | <-------------------- |
          |                      |                       |

```

However, the current model gives some advantages against the single-thread
implementation, as each thread can handle a share of the total number of
connected client effectively increasing the throughput of the broker, the
main drawback is that in worst cases a scenario that can originate is where some
threads are serving heavy-load clients and some others are esentially idling
without partecipating or helping.

## Benchmarks

I've run some basic benchmarks comparing Sol and Mosquitto's performance, using
[mqtt-benchmark](https://github.com/krylovsk/mqtt-benchmark) tool to test
concurrent connections and different loads of traffic on my laptop, an
Intel(R) I5-8265U (8) @ 3.900 GHz CPUs and 8 GB RAM.

To be minded that currently Sol skips some checks like UTF-8 strings for topics
and the memory footprint is also much greater than Mosquitto's one; moreover
this benchmarks doesn't take into account real publish on multiple subscribers,
just the publish traffic load on multiple clients with different QoS levels.

![MQTT mosquitto vs sol comparison](MQTTcomparison.png)

All tests are run pointing to localhost:1883, apparently, out of QoS 0 tests
which are the least interesting as there's no real "handshakes" or overhead on
the communication between the peers, QoS 1 and even more QoS 2 is where Sol
seems to shine, highlighting a 95% increased throughtput over Mosquitto on the
QoS 2 1000 clients benchmark.
Of course these benchmarks are of little relevance and Mosquitto is compiled
without any optimizations, just an out of the box version binary shipped by
the OS repository, version 1.6.8, but in terms of sheer concurrency Sol does
pretty good.

## Contributing

Pull requests are welcome, just create an issue and fork it.
