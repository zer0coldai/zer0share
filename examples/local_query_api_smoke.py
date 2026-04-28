from __future__ import annotations

import argparse
import sys

from zer0share import pro_api


def _print_frame(title, df, rows: int = 5) -> None:
    print(f"\n## {title}")
    print(f"rows: {len(df)}")
    if df.empty:
        print("(empty)")
        return
    print(df.head(rows).to_string(index=False))


def run_examples(ts_code: str, start_date: str, end_date: str, trade_date: str) -> None:
    pro = pro_api()

    basic = pro.stock_basic(
        ts_code=ts_code,
        fields="ts_code,symbol,name,market,exchange,list_status,list_date,delist_date",
    )
    _print_frame("stock_basic", basic)

    trade_cal = pro.trade_cal(
        exchange="SSE",
        start_date=start_date,
        end_date=end_date,
        is_open="1",
        fields="exchange,cal_date,is_open,pretrade_date",
    )
    _print_frame("trade_cal", trade_cal)

    daily = pro.daily(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        fields="ts_code,trade_date,open,high,low,close,vol,amount",
    )
    _print_frame("daily", daily)

    adj_factor = pro.adj_factor(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        fields="ts_code,trade_date,adj_factor",
    )
    _print_frame("adj_factor", adj_factor)

    qfq = pro.pro_bar(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        adj="qfq",
    )
    _print_frame("pro_bar_qfq", qfq)

    hfq = pro.pro_bar(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        adj="hfq",
    )
    _print_frame("pro_bar_hfq", hfq)

    by_query = pro.query(
        "pro_bar",
        ts_code=ts_code,
        trade_date=trade_date,
        adj="qfq",
    )
    _print_frame("query_pro_bar_qfq_by_trade_date", by_query)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke-test the local Tushare-like query API against synced Parquet data."
    )
    parser.add_argument("--ts-code", default="000001.SZ")
    parser.add_argument("--start-date", default="20240101")
    parser.add_argument("--end-date", default="20240331")
    parser.add_argument("--trade-date", default="20240102")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        run_examples(
            ts_code=args.ts_code,
            start_date=args.start_date,
            end_date=args.end_date,
            trade_date=args.trade_date,
        )
    except FileNotFoundError as exc:
        print(f"Missing local data: {exc}", file=sys.stderr)
        print("Run the relevant sync command first, for example:", file=sys.stderr)
        print("  uv run python main.py sync --all", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
