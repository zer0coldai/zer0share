from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from src.config import load_config
from src.fetcher import ADJ_FACTOR_COLS, BASIC_COLS, DAILY_COLS, TRADE_CAL_COLS


class LocalPro:
    def __init__(self, data_dir: str | Path):
        self._data_dir = Path(data_dir)

    def stock_basic(
        self,
        ts_code: str | None = None,
        name: str | None = None,
        market: str | None = None,
        list_status: str | None = "L",
        exchange: str | None = None,
        is_hs: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        path = self._data_dir / "basic" / "data.parquet"
        if not path.exists():
            raise FileNotFoundError("basic data not found; run `python main.py sync --table basic` first")

        columns = _parse_fields(fields, BASIC_COLS)
        where = []
        params = []
        if ts_code is not None:
            where.append("ts_code = ?")
            params.append(ts_code)
        if name is not None:
            where.append("name = ?")
            params.append(name)
        if market is not None:
            where.append("market = ?")
            params.append(market)
        if list_status is not None:
            where.append("list_status = ?")
            params.append(list_status)
        if exchange is not None:
            where.append("exchange = ?")
            params.append(exchange)
        if is_hs is not None:
            where.append("is_hs = ?")
            params.append(is_hs)

        sql = f"SELECT {', '.join(columns)} FROM read_parquet(?)"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY ts_code"

        df = duckdb.connect().execute(sql, [str(path), *params]).fetchdf()
        return _format_date_columns(df, ["list_date", "delist_date"])

    def trade_cal(
        self,
        exchange: str = "SSE",
        start_date: str | None = None,
        end_date: str | None = None,
        is_open: str | int | bool | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        trade_cal_dir = self._data_dir / "trade_cal"
        if not trade_cal_dir.exists():
            raise FileNotFoundError(
                "trade_cal data not found; run `python main.py sync --table trade_cal` first"
            )

        columns = _parse_fields(fields, TRADE_CAL_COLS)
        where = ["exchange = ?"]
        params = [exchange]
        parsed_start = _parse_date(start_date) if start_date is not None else None
        parsed_end = _parse_date(end_date) if end_date is not None else None
        if parsed_start is not None and parsed_end is not None and parsed_end < parsed_start:
            raise ValueError("end_date must be on or after start_date")
        if parsed_start is not None:
            where.append("cal_date >= ?")
            params.append(parsed_start)
        if parsed_end is not None:
            where.append("cal_date <= ?")
            params.append(parsed_end)
        if is_open is not None:
            where.append("is_open = ?")
            params.append(_parse_is_open(is_open))

        pattern = trade_cal_dir / "exchange=*" / "data.parquet"
        sql = (
            f"SELECT {', '.join(columns)} FROM read_parquet(?, hive_partitioning=true) "
            f"WHERE {' AND '.join(where)} ORDER BY exchange, cal_date"
        )
        df = duckdb.connect().execute(sql, [str(pattern), *params]).fetchdf()
        return _format_date_columns(df, ["cal_date", "pretrade_date"])

    def daily(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        return self._query_daily_partitioned(
            table_name="daily_kline",
            sync_table="daily_kline",
            columns=DAILY_COLS,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
        )

    def adj_factor(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        return self._query_daily_partitioned(
            table_name="adj_factor",
            sync_table="adj_factor",
            columns=ADJ_FACTOR_COLS,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
        )

    def pro_bar(
        self,
        ts_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        asset: str = "E",
        adj: str | None = None,
        freq: str = "D",
        trade_date: str | None = None,
        ma: list[int] | None = None,
    ) -> pd.DataFrame:
        if asset != "E":
            raise NotImplementedError("local pro_bar currently only supports asset='E'")
        if freq != "D":
            raise NotImplementedError("local pro_bar currently only supports freq='D'")
        if ma:
            raise NotImplementedError("local pro_bar does not support ma yet")
        if adj not in (None, "qfq", "hfq"):
            raise ValueError("adj must be one of None, 'qfq', or 'hfq'")

        daily = self.daily(
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )
        if adj is None or daily.empty:
            return daily

        factors = self.adj_factor(
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )
        if factors.empty:
            return daily.iloc[0:0].copy()

        result = daily.merge(
            factors[["ts_code", "trade_date", "adj_factor"]],
            on=["ts_code", "trade_date"],
            how="left",
        ).sort_values(["ts_code", "trade_date"])
        result["adj_factor"] = result.groupby("ts_code")["adj_factor"].bfill()
        result = result.dropna(subset=["adj_factor"])
        if result.empty:
            return daily.iloc[0:0].copy()

        price_columns = ["open", "high", "low", "close", "pre_close"]
        if adj == "qfq":
            base_factor = result.sort_values("trade_date").iloc[-1]["adj_factor"]
            multiplier = result["adj_factor"] / base_factor
        else:
            multiplier = result["adj_factor"]

        for column in price_columns:
            result[column] = (result[column] * multiplier).round(2)

        result["change"] = (result["close"] - result["pre_close"]).round(2)
        result["pct_chg"] = (result["change"] / result["pre_close"] * 100).round(2)

        return result.drop(columns=["adj_factor"])

    def query(self, api_name: str, **kwargs) -> pd.DataFrame:
        dispatch = {
            "stock_basic": self.stock_basic,
            "trade_cal": self.trade_cal,
            "daily": self.daily,
            "adj_factor": self.adj_factor,
            "pro_bar": self.pro_bar,
        }
        try:
            method = dispatch[api_name]
        except KeyError as e:
            raise ValueError(f"unknown api: {api_name}") from e
        return method(**kwargs)

    def _query_daily_partitioned(
        self,
        table_name: str,
        sync_table: str,
        columns: list[str],
        ts_code: str | None,
        trade_date: str | None,
        start_date: str | None,
        end_date: str | None,
        fields: str | list[str] | None,
    ) -> pd.DataFrame:
        if trade_date is not None and (start_date is not None or end_date is not None):
            raise ValueError("trade_date cannot be combined with start_date or end_date")
        parsed_start = _parse_date(start_date) if start_date is not None else None
        parsed_end = _parse_date(end_date) if end_date is not None else None
        if parsed_start is not None and parsed_end is not None and parsed_end < parsed_start:
            raise ValueError("end_date must be on or after start_date")

        table_dir = self._data_dir / table_name
        if not table_dir.exists():
            raise FileNotFoundError(
                f"{sync_table} data not found; run `python main.py sync --table {sync_table}` first"
            )

        selected = _parse_fields(fields, columns)
        where = []
        params = []
        if ts_code is not None:
            codes = [code.strip() for code in ts_code.split(",") if code.strip()]
            placeholders = ", ".join("?" for _ in codes)
            where.append(f"ts_code IN ({placeholders})")
            params.extend(codes)
        if trade_date is not None:
            where.append("trade_date = ?")
            params.append(_parse_date(trade_date))
        if parsed_start is not None:
            where.append("trade_date >= ?")
            params.append(parsed_start)
        if parsed_end is not None:
            where.append("trade_date <= ?")
            params.append(parsed_end)

        pattern = table_dir / "date=*" / "data.parquet"
        sql = f"SELECT {', '.join(selected)} FROM read_parquet(?, hive_partitioning=true)"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY ts_code, trade_date"

        df = duckdb.connect().execute(sql, [str(pattern), *params]).fetchdf()
        return _format_date_columns(df, ["trade_date"])


def pro_api(config_path: str | Path = "config/settings.toml") -> LocalPro:
    cfg = load_config(Path(config_path))
    return LocalPro(cfg.data_dir)


def _parse_fields(fields: str | list[str] | None, default_columns: list[str]) -> list[str]:
    if fields is None:
        return list(default_columns)
    if isinstance(fields, str):
        parsed = [field.strip() for field in fields.split(",") if field.strip()]
    else:
        parsed = list(fields)
    unknown = [field for field in parsed if field not in default_columns]
    if unknown:
        raise ValueError(f"unknown fields: {', '.join(unknown)}")
    return parsed


def _parse_date(value: str):
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"invalid date format: {value}")


def _parse_is_open(value: str | int | bool) -> bool:
    if isinstance(value, bool):
        return value
    if value in (1, "1"):
        return True
    if value in (0, "0"):
        return False
    raise ValueError("is_open must be one of True, False, 1, 0, '1', or '0'")


def _format_date_columns(df: pd.DataFrame, date_columns: list[str]) -> pd.DataFrame:
    for column in date_columns:
        if column not in df.columns:
            continue
        formatted = pd.to_datetime(df[column], errors="coerce").dt.strftime("%Y%m%d")
        df[column] = formatted.astype(object)
        df.loc[formatted.isna(), column] = None
    return df
