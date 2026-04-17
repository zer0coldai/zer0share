# 数据同步指南

## 前置条件

### 1. 安装依赖

```bash
uv sync --dev
```

### 2. 配置文件

复制示例配置并填写真实参数：

```bash
cp config/settings.example.toml config/settings.toml
```

编辑 `config/settings.toml`：

```toml
[tushare]
token = "你的 Tushare Pro Token"

[paths]
data_dir = "data"
db_path = "db/meta.duckdb"
log_path = "logs/pipeline.log"

[scheduler]
daily_kline_hour = 18
daily_kline_minute = 0
basic_hour = 8

[notifier]
wecom_webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY"
enabled = false
```

> Tushare Token 在 [tushare.pro](https://tushare.pro) 注册后获取，需要积分 >= 2000 才能调用 `daily` 接口。

---

## 首次同步

### 步骤一：同步交易日历

交易日历是其他同步的前置依赖，**必须最先执行**。

```bash
uv run python main.py sync --table trade_cal
```

此命令会：
- 拉取 SSE、SZSE、CFFEX、SHFE、CZCE、DCE、INE 共 7 个交易所从 1990-01-01 至今的全量日历
- 写入 `data/trade_cal/exchange=XXX/data.parquet`
- 加载到 DuckDB 供后续查询

预计耗时：1～3 分钟（受网络和 Tushare 限速影响）。

### 步骤二：同步股票基础信息

```bash
uv run python main.py sync --table basic
```

此命令会：
- 拉取全市场所有状态（上市 L、退市 D、暂停 P、精选层 G）的股票基础信息
- 写入 `data/basic/data.parquet`

### 步骤三：同步日线行情

```bash
uv run python main.py sync --table daily_kline
```

此命令会：
- 以 SSE 交易日历为基准，只对真实交易日拉取数据（跳过周末和节假日）
- 从 2016-01-01 起增量同步到今天
- 每个交易日写入 `data/daily_kline/date=YYYYMMDD/data.parquet`

> **注意**：首次同步历史数据量较大（约 10 年 × 3800 只股票），耗时可能在 1～2 小时，受 Tushare 每分钟调用频次限制影响。

---

## 一键同步全部

以上三步可合并为一条命令，顺序固定为 trade_cal → basic → daily_kline：

```bash
uv run python main.py sync --all
```

---

## 查看同步状态

```bash
uv run python main.py status
```

输出示例：

```
trade_cal    last sync: 2026-04-17
basic        last sync: 2026-04-17
daily_kline  last sync: 2026-04-17
```

---

## 增量更新

再次运行任意 `sync` 命令时，pipeline 会自动从上次同步的日期之后继续拉取，无需重新全量同步。

```bash
# 每日收盘后更新日线行情
uv run python main.py sync --table daily_kline
```

---

## 自动化调度

启动后台定时任务，按配置自动在收盘后同步：

```bash
uv run python main.py scheduler start
```

默认调度时间（可在 `settings.toml` 修改）：

| 任务 | 时间 |
|------|------|
| daily_kline | 每个工作日 18:00 |
| basic | 每个工作日 08:00 |

> 调度器需保持进程运行。生产环境建议配合 `systemd` 或 `supervisor` 管理进程。

---

## 数据目录结构

同步完成后，本地数据布局如下：

```
data/
├── trade_cal/
│   ├── exchange=SSE/data.parquet
│   ├── exchange=SZSE/data.parquet
│   ├── exchange=CFFEX/data.parquet
│   ├── exchange=SHFE/data.parquet
│   ├── exchange=CZCE/data.parquet
│   ├── exchange=DCE/data.parquet
│   └── exchange=INE/data.parquet
├── basic/
│   └── data.parquet
└── daily_kline/
    ├── date=20160104/data.parquet
    ├── date=20160105/data.parquet
    └── ...
db/
└── meta.duckdb
logs/
└── pipeline.log
```
