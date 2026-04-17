# zer0share

A-股数据本地化管道，基于 [Tushare Pro](https://tushare.pro) 拉取股票数据，以 Parquet 分区存储，DuckDB 提供快速元数据查询，支持增量同步与定时调度。

## 特性

- **交易日历**：拉取 7 个交易所完整日历，增量同步自动跳过周末与节假日
- **股票基础信息**：全市场上市 / 退市 / 暂停 / 精选层股票完整字段
- **日线行情**：按交易日分区存储，支持增量续传
- **本地优先**：Parquet 分区文件 + DuckDB，无需数据库服务
- **定时调度**：APScheduler 驱动，收盘后自动触发同步
- **企业微信通知**：同步失败可推送告警（可选）

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
```

### 4. 查看同步状态

```bash
uv run python main.py status
```

### 5. 启动定时调度

```bash
uv run python main.py scheduler start
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
└── daily_kline/
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
├── fetcher.py    # Tushare API 封装
├── storage.py    # Parquet 读写 + DuckDB MetaStore
├── pipeline.py   # 同步业务逻辑
├── scheduler.py  # APScheduler 定时任务
├── notifier.py   # 企业微信 Webhook 通知
└── cli.py        # Click CLI 入口
tests/            # pytest 测试套件
config/
└── settings.example.toml
```

## License

MIT
