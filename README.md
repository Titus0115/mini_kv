# mini-kv

轻量级嵌入式 KV 存储引擎，纯 Rust 实现。

## 特性

- **WAL 持久化**：预写日志 + CRC32 校验，数据不丢失
- **TTL 过期**：支持毫秒级 key 过期，后台自动清理
- **前缀扫描**：利用 BTreeMap 有序性高效前缀匹配
- **HTTP API**：基于 axum 的 REST 接口
- **可观测性**：Prometheus 指标 + 结构化 JSON 日志 + X-Request-Id
- **优雅关闭**：SIGTERM/SIGINT 信号处理

## 项目结构

mini-kv/
├── Cargo.toml
├── crates/
│   ├── mini-kv-core/      # 核心存储引擎（WAL、内存索引、Janitor）
│   ├── mini-kv-server/     # HTTP API 服务（axum）
│   ├── mini-kv-client/     # Rust 客户端库
│   └── mini-kv-cli/        # 命令行工具

## 快速开始

### 构建

cargo build --release --bin mini-kv-server --bin mini-kv-cli

### 启动服务

cargo run --release --bin mini-kv-server -- --data-dir ./data --port 3456 --log-level info

### 使用 CLI

cargo run --bin mini-kv-cli -- --server http://127.0.0.1:3456 put mykey myvalue --ttl-secs 60
cargo run --bin mini-kv-cli -- --server http://127.0.0.1:3456 get mykey
cargo run --bin mini-kv-cli -- --server http://127.0.0.1:3456 prefix user:
cargo run --bin mini-kv-cli -- --server http://127.0.0.1:3456 health
cargo run --bin mini-kv-cli -- --server http://127.0.0.1:3456 flush

### 使用 curl

# 写入（带 TTL）
curl -X POST http://127.0.0.1:3456/kv/mykey \
  -H 'Content-Type: application/json' \
  -d '{"value":"hello","ttl_secs":60}'

# 读取
curl http://127.0.0.1:3456/kv/mykey

# 删除
curl -X DELETE http://127.0.0.1:3456/kv/mykey

# 前缀扫描
curl "http://127.0.0.1:3456/kv/prefix?prefix=user:"

# 范围扫描
curl "http://127.0.0.1:3456/kv?start=a&end=z"

# 健康检查
curl http://127.0.0.1:3456/health

# Prometheus 指标
curl http://127.0.0.1:3456/metrics

# 刷新 WAL
curl -X POST http://127.0.0.1:3456/flush

## API 参考

| 端点 | 方法 | 说明 |
|------|------|------|
| /kv/{key} | GET | 获取键值 |
| /kv/{key} | POST | 写入键值 |
| /kv/{key} | DELETE | 删除键 |
| /kv | GET | 范围扫描（?start=x&end=y） |
| /kv/prefix | GET | 前缀扫描（?prefix=x） |
| /health | GET | 健康检查 |
| /metrics | GET | Prometheus 指标 |
| /flush | POST | 刷新 WAL |

## 配置

| 参数 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| --data-dir | MINI_KV_DATA_DIR | ./data | 数据存储目录 |
| --host | MINI_KV_HOST | 127.0.0.1 | 监听地址 |
| --port | MINI_KV_PORT | 3456 | 监听端口 |
| --log-level | MINI_KV_LOG | error | 日志级别 |
| --wal-threshold-bytes | - | 67108864 | WAL 触发 compaction 阈值 |
| --janitor-interval-secs | - | 30 | 过期 key 清理间隔 |

## 开发

# 编译检查
cargo check --all

# 运行测试
cargo test --all

# Lint
cargo clippy --all -- -D warnings

# 格式检查
cargo fmt --check