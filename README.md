# zer0share

![Python](https://img.shields.io/badge/python-3.11+-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Stars](https://img.shields.io/github/stars/zer0coldai/zer0share)

> **zer0share** — A local data pipeline for Chinese A-share market.  
> Pulls data from Tushare Pro, stores as Parquet partitions,  
> queries via DuckDB, with incremental sync & APScheduler automation.

A-股数据本地化管道，基于 [Tushare Pro](https://tushare.pro) 拉取股票数据，以 Parquet 分区存储，DuckDB 提供快速元数据查询，支持增量同步与定时调度。

## 特性

- **核心数据同步**：支持交易日历、股票基础信息、日线行情、复权因子
- **本地优先存储**：Parquet 分区文件 + DuckDB 元数据，无需数据库服务
- **Tushare-like 查询**：本地 `pro_api()` 直接返回 DataFrame，不消耗 Tushare 积分
- **复权行情**：本地 `pro_bar()` 支持不复权、前复权（qfq）和后复权（hfq）
- **自动化运维**：APScheduler 定时同步，支持企业微信失败告警

## 环境要求

- Python 3.11+
- [uv](https://github.com/astral-sh/uv)
- Tushare Pro Token（需积分 ≥ 2000）

## 快速开始

### 1. 克隆并安装依赖

```bash
git clone https://github.com/your-username/zer0share.git
cd zer0share
uv sync
```

### 2. 配置

```bash
cp config/settings.example.toml config/settings.toml
```

编辑 `config/settings.toml`，填入 Tushare Token：

```toml
[tushare]
token = "your_tushare_token_here"
```

### 3. 首次同步

```bash
# 一键同步全部（推荐）
uv run python main.py sync --all

# 或逐步执行（顺序不可颠倒）
uv run python main.py sync --table trade_cal   # 交易日历（必须最先）
uv run python main.py sync --table basic       # 股票基础信息
uv run python main.py sync --table daily_kline # 日线行情（依赖交易日历）
uv run python main.py sync --table adj_factor  # 复权因子（依赖交易日历）
```

### 4. 查看同步状态

```bash
uv run python main.py status
```

### 5. 启动定时调度

```bash
uv run python main.py scheduler start
```

## 本地查询 API

同步完成后，可以在研究代码中使用类似 Tushare Pro 的本地 Python API 查询数据。查询只读取本地 Parquet 文件，通过 DuckDB 执行，不会访问 Tushare，也不会消耗积分。

```python
from src import pro_api

pro = pro_api()

basic = pro.stock_basic(list_status="L")
cal = pro.trade_cal(exchange="SSE", start_date="20240101", end_date="20240131")
daily = pro.daily(ts_code="000001.SZ", start_date="20240101", end_date="20240331")
adj = pro.adj_factor(ts_code="000001.SZ", start_date="20240101", end_date="20240331")

qfq = pro.pro_bar(
    ts_code="000001.SZ",
    start_date="20240101",
    end_date="20240331",
    adj="qfq",
)
```

支持的本地查询方法：

| 方法 | 说明 |
|------|------|
| `stock_basic` | 查询已同步的股票基础信息 |
| `trade_cal` | 查询已同步的交易日历 |
| `daily` | 查询已同步的 A 股日线行情 |
| `adj_factor` | 查询已同步的复权因子 |
| `pro_bar` | 查询本地 A 股日线行情，支持不复权、前复权（qfq）和后复权（hfq） |
| `query` | 按接口名分发，例如 `pro.query("daily", ...)` |

运行示例：

```bash
uv run python examples/local_query_api_smoke.py
```

## 数据存储结构

```
data/
├── trade_cal/
│   ├── exchange=SSE/data.parquet
│   ├── exchange=SZSE/data.parquet
│   └── ...                          # CFFEX / SHFE / CZCE / DCE / INE
├── basic/
│   └── data.parquet
├── daily_kline/
│   ├── date=20160104/data.parquet
│   ├── date=20160105/data.parquet
│   └── ...
└── adj_factor/
    ├── date=20160104/data.parquet
    ├── date=20160105/data.parquet
    └── ...
db/
└── meta.duckdb                      # 同步记录 + 交易日历索引
```

## CLI 命令

| 命令 | 说明 |
|------|------|
| `sync --table trade_cal` | 同步交易日历（7 个交易所） |
| `sync --table basic` | 同步股票基础信息 |
| `sync --table daily_kline` | 增量同步日线行情 |
| `sync --table adj_factor` | 增量同步复权因子 |
| `sync --all` | 按顺序同步全部 |
| `status` | 查看各表最后同步时间 |
| `scheduler start` | 启动定时调度 |

## 配置说明

```toml
[tushare]
token = "your_tushare_token_here"

[paths]
data_dir = "data"          # Parquet 存储目录
db_path = "db/meta.duckdb" # DuckDB 文件路径
log_path = "logs/pipeline.log"

[scheduler]
daily_kline_hour = 18      # 日线同步触发时间（小时）
daily_kline_minute = 0
basic_hour = 8             # 基础信息同步触发时间（小时）
adj_factor_hour = 18       # 复权因子同步触发时间（小时）
adj_factor_minute = 5

[notifier]
wecom_webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY"
enabled = false            # 填写真实 webhook_url 后改为 true
```

## 开发

```bash
# 安装开发依赖
uv sync --dev

# 运行测试
uv run pytest

# 运行单个测试文件
uv run pytest tests/test_pipeline.py -v
```

## 项目结构

```
src/
├── config.py     # 配置加载
├── api.py        # 本地 Tushare-like 查询 API
├── fetcher.py    # Tushare API 封装
├── storage.py    # Parquet 读写 + DuckDB MetaStore
├── pipeline.py   # 同步业务逻辑
├── scheduler.py  # APScheduler 定时任务
├── notifier.py   # 企业微信 Webhook 通知
└── cli.py        # Click CLI 入口
tests/            # pytest 测试套件
examples/         # 本地查询 API 示例
config/
└── settings.example.toml
```

## License

MIT
