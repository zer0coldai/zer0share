import tushare as ts
import pandas as pd
from datetime import date
from loguru import logger


BASIC_COLS = [
    "ts_code",
    "symbol",
    "name",
    "area",
    "industry",
    "fullname",
    "enname",
    "cnspell",
    "market",
    "exchange",
    "curr_type",
    "list_status",
    "list_date",
    "delist_date",
    "is_hs",
    "act_name",
    "act_ent_type",
]
DAILY_COLS = [
    "ts_code", "trade_date", "open", "high", "low",
    "close", "pre_close", "change", "pct_chg", "vol", "amount"
]
TRADE_CAL_COLS = ["exchange", "cal_date", "is_open", "pretrade_date"]
ADJ_FACTOR_COLS = ["ts_code", "trade_date", "adj_factor"]


class TushareFetcher:
    def __init__(self, token: str):
        self._pro = ts.pro_api(token)

    def fetch_basic(self) -> pd.DataFrame:
        logger.info("拉取 stock_basic")
        df = self._pro.stock_basic(
            exchange="",
            list_status="L,D,P,G",
            fields=",".join(BASIC_COLS)
        )
        df["list_date"] = pd.to_datetime(
            df["list_date"], format="%Y%m%d", errors="coerce"
        ).dt.date
        df["delist_date"] = pd.to_datetime(
            df["delist_date"], format="%Y%m%d", errors="coerce"
        ).apply(lambda x: x.date() if not pd.isnull(x) else None)
        return df[BASIC_COLS]

    def fetch_daily_kline(self, trade_date: date) -> pd.DataFrame:
        date_str = trade_date.strftime("%Y%m%d")
        logger.info(f"拉取日线行情: {date_str}")
        df = self._pro.daily(trade_date=date_str, fields=",".join(DAILY_COLS))
        if df is None or df.empty:
            return pd.DataFrame(columns=DAILY_COLS)
        df["trade_date"] = pd.to_datetime(
            df["trade_date"], format="%Y%m%d"
        ).dt.date
        return df[DAILY_COLS]

    def fetch_adj_factor(self, trade_date: date) -> pd.DataFrame:
        date_str = trade_date.strftime("%Y%m%d")
        logger.info(f"拉取复权因子: {date_str}")
        df = self._pro.adj_factor(trade_date=date_str, fields=",".join(ADJ_FACTOR_COLS))
        if df is None or df.empty:
            return pd.DataFrame(columns=ADJ_FACTOR_COLS)
        df["trade_date"] = pd.to_datetime(
            df["trade_date"], format="%Y%m%d"
        ).dt.date
        return df[ADJ_FACTOR_COLS]

    def fetch_trade_cal(self, exchange: str) -> pd.DataFrame:
        today = date.today().strftime("%Y%m%d")
        logger.info(f"拉取交易日历: {exchange}")
        df = self._pro.trade_cal(
            exchange=exchange,
            start_date="19900101",
            end_date=today,
            fields=",".join(TRADE_CAL_COLS),
        )
        if df is None or df.empty:
            return pd.DataFrame(columns=TRADE_CAL_COLS)
        df["cal_date"] = pd.to_datetime(
            df["cal_date"], format="%Y%m%d", errors="coerce"
        ).dt.date
        df["pretrade_date"] = pd.to_datetime(
            df["pretrade_date"], format="%Y%m%d", errors="coerce"
        ).apply(lambda x: x.date() if not pd.isnull(x) else None)
        df["is_open"] = df["is_open"].astype(str).map({"1": True, "0": False}).astype(object)
        return df[TRADE_CAL_COLS]
