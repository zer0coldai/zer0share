# Examples

Run these examples from the repository root after syncing local data.

```bash
uv run python examples/local_query_api_smoke.py
```

Use a different security or date range:

```bash
uv run python examples/local_query_api_smoke.py --ts-code 600000.SH --start-date 20240101 --end-date 20240331
```

The example calls the local Tushare-like API:

- `pro.stock_basic`
- `pro.trade_cal`
- `pro.daily`
- `pro.adj_factor`
- `pro.query`

It reads local Parquet data only. It does not call Tushare.
