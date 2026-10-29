# 测试目录

测试按执行方式和覆盖范围分组：

| 目录 | 内容 | 入口 |
|---|---|---|
| `unit/` | 容器与基础数据结构单测 | CTest `unit`，生成 `sol_test` |
| `protocol/` | MQTT 编解码、topic 匹配、配置和工具函数单测 | CTest `proto`，生成 `sol_proto_test` |
| `fuzz/` | `mqtt_unpack()` 的 libFuzzer harness 与种子语料 | `-DFUZZ=ON` 后生成 `sol_fuzz` |
| `integration/` | 启动真实 broker 的 Python 端到端测试及测试配置 | CTest `integration` |
| `tools/` | TLS 证书生成、压力工具、TSan 测试包装器 | 手动运行，不属于快速单测 |

从干净构建开始运行完整套件：

```sh
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

集成测试在仓库根目录运行，以便共享 `SOL_BIN`、配置和生成的 `certs/`。
`examples/` 下脚本是人工演示用客户端，不代替这些自动化回归测试。
