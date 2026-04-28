from datetime import date

import pandas as pd
import pytest

from zer0share.storage import MetaStore, daily_kline_partition_exists, read_basic, read_daily_kline, write_basic, write_daily_kline, write_trade_cal, read_trade_cal


FULL_BASIC_COLUMNS = [
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


def _basic_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "symbol": ["000001"],
            "name": ["平安银行"],
            "area": ["深圳"],
            "industry": ["银行"],
            "fullname": ["平安银行股份有限公司"],
            "enname": ["Ping An Bank"],
            "cnspell": ["payh"],
            "market": ["主板"],
            "exchange": ["SZSE"],
            "curr_type": ["CNY"],
            "list_status": ["L"],
            "list_date": [date(1991, 4, 3)],
            "delist_date": [None],
            "is_hs": ["S"],
            "act_name": ["深圳市投资控股有限公司"],
            "act_ent_type": ["地方国企"],
        }
    )


def _basic_df_two_rows() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ"],
            "symbol": ["000001", "000002"],
            "name": ["平安银行", "万科A"],
            "area": ["深圳", "深圳"],
            "industry": ["银行", "房地产"],
            "fullname": ["平安银行股份有限公司", "万科企业股份有限公司"],
            "enname": ["Ping An Bank", "China Vanke Co., Ltd."],
            "cnspell": ["payh", "wka"],
            "market": ["主板", "主板"],
            "exchange": ["SZSE", "SZSE"],
            "curr_type": ["CNY", "CNY"],
            "list_status": ["L", "L"],
            "list_date": [date(1991, 4, 3), date(1991, 1, 29)],
            "delist_date": [None, None],
            "is_hs": ["S", "S"],
            "act_name": ["深圳市投资控股有限公司", "深圳地铁集团有限公司"],
            "act_ent_type": ["地方国企", "地方国企"],
        }
    )


@pytest.fixture
def store(tmp_path):
    s = MetaStore(tmp_path / "meta.duckdb")
    yield s
    s.close()


def test_init_creates_table(store):
    assert store.get_last_date("daily_kline") is None


def test_update_and_get_last_date(store):
    store.update_last_date("daily_kline", date(2024, 1, 15))
    assert store.get_last_date("daily_kline") == date(2024, 1, 15)


def test_update_overwrites_previous(store):
    store.update_last_date("daily_kline", date(2024, 1, 1))
    store.update_last_date("daily_kline", date(2024, 1, 31))
    assert store.get_last_date("daily_kline") == date(2024, 1, 31)


def test_different_table_names_are_independent(store):
    store.update_last_date("daily_kline", date(2024, 1, 10))
    store.update_last_date("basic", date(2024, 2, 20))
    assert store.get_last_date("daily_kline") == date(2024, 1, 10)
    assert store.get_last_date("basic") == date(2024, 2, 20)


def test_context_manager(tmp_path):
    with MetaStore(tmp_path / "meta.duckdb") as store:
        store.update_last_date("daily_kline", date(2024, 1, 1))
        assert store.get_last_date("daily_kline") == date(2024, 1, 1)


def test_write_and_read_daily_kline(tmp_path):
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ"],
            "trade_date": [date(2024, 1, 2), date(2024, 1, 2)],
            "open": [10.0, 20.0],
            "high": [11.0, 21.0],
            "low": [9.5, 19.5],
            "close": [10.5, 20.5],
            "pre_close": [10.0, 20.0],
            "change": [0.5, 0.5],
            "pct_chg": [5.0, 2.5],
            "vol": [100000.0, 200000.0],
            "amount": [1050000.0, 4100000.0],
        }
    )
    write_daily_kline(tmp_path, date(2024, 1, 2), df)
    result = read_daily_kline(tmp_path, date(2024, 1, 2))
    assert len(result) == 2
    assert set(result["ts_code"]) == {"000001.SZ", "000002.SZ"}


def test_daily_kline_partition_path(tmp_path):
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": [date(2024, 1, 2)],
            "open": [10.0],
            "high": [11.0],
            "low": [9.5],
            "close": [10.5],
            "pre_close": [10.0],
            "change": [0.5],
            "pct_chg": [5.0],
            "vol": [100000.0],
            "amount": [1050000.0],
        }
    )
    write_daily_kline(tmp_path, date(2024, 1, 2), df)
    assert (tmp_path / "daily_kline" / "date=20240102" / "data.parquet").exists()


def test_daily_kline_partition_exists(tmp_path):
    assert daily_kline_partition_exists(tmp_path, date(2024, 1, 2)) is False

    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": [date(2024, 1, 2)],
            "open": [10.0],
            "high": [11.0],
            "low": [9.5],
            "close": [10.5],
            "pre_close": [10.0],
            "change": [0.5],
            "pct_chg": [5.0],
            "vol": [100000.0],
            "amount": [1050000.0],
        }
    )
    write_daily_kline(tmp_path, date(2024, 1, 2), df)

    assert daily_kline_partition_exists(tmp_path, date(2024, 1, 2)) is True


def test_write_and_read_basic(tmp_path):
    df = _basic_df()
    write_basic(tmp_path, df)
    result = read_basic(tmp_path)
    assert len(result) == 1
    assert list(result.columns) == FULL_BASIC_COLUMNS
    assert result.iloc[0]["name"] == "平安银行"
    assert result.iloc[0]["fullname"] == "平安银行股份有限公司"


def test_basic_overwrites_on_second_write(tmp_path):
    write_basic(tmp_path, _basic_df())
    write_basic(tmp_path, _basic_df_two_rows())
    result = read_basic(tmp_path)
    assert len(result) == 2
    assert list(result.columns) == FULL_BASIC_COLUMNS
    assert set(result["ts_code"]) == {"000001.SZ", "000002.SZ"}


def test_read_daily_kline_returns_empty_if_not_exists(tmp_path):
    result = read_daily_kline(tmp_path, date(2024, 1, 2))
    assert result.empty


def test_read_basic_returns_empty_if_not_exists(tmp_path):
    result = read_basic(tmp_path)
    assert result.empty


def test_write_and_read_trade_cal(tmp_path):
    df = pd.DataFrame({
        "exchange": ["SSE", "SSE"],
        "cal_date": [date(2024, 1, 2), date(2024, 1, 3)],
        "is_open": [True, False],
        "pretrade_date": [date(2023, 12, 29), date(2024, 1, 2)],
    })
    write_trade_cal(tmp_path, "SSE", df)
    result = read_trade_cal(tmp_path, "SSE")
    assert len(result) == 2
    assert (tmp_path / "trade_cal" / "exchange=SSE" / "data.parquet").exists()


def test_read_trade_cal_returns_empty_if_not_exists(tmp_path):
    result = read_trade_cal(tmp_path, "SSE")
    assert result.empty


def test_load_trade_cal_from_parquet(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    df = pd.DataFrame({
        "exchange": ["SSE", "SSE"],
        "cal_date": [date(2024, 1, 2), date(2024, 1, 3)],
        "is_open": [True, False],
        "pretrade_date": [date(2023, 12, 29), date(2024, 1, 2)],
    })
    write_trade_cal(tmp_path, "SSE", df)
    with MetaStore(db_path) as store:
        store.load_trade_cal_from_parquet(tmp_path)
        row = store._conn.execute(
            "SELECT COUNT(*) FROM trade_cal WHERE exchange='SSE'"
        ).fetchone()
        assert row[0] == 2


def test_get_trading_days(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    df = pd.DataFrame({
        "exchange": ["SSE"] * 5,
        "cal_date": [
            date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4),
            date(2024, 1, 5), date(2024, 1, 6),
        ],
        "is_open": [True, False, True, False, True],
        "pretrade_date": [
            date(2023, 12, 29), date(2024, 1, 2), date(2024, 1, 2),
            date(2024, 1, 4), date(2024, 1, 4),
        ],
    })
    write_trade_cal(tmp_path, "SSE", df)
    with MetaStore(db_path) as store:
        store.load_trade_cal_from_parquet(tmp_path)
        days = store.get_trading_days("SSE", date(2024, 1, 1), date(2024, 1, 6))
    assert days == [date(2024, 1, 2), date(2024, 1, 4), date(2024, 1, 6)]


def test_get_trading_days_returns_empty_when_no_cal(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    with MetaStore(db_path) as store:
        days = store.get_trading_days("SSE", date(2024, 1, 1), date(2024, 1, 6))
    assert days == []


def test_get_trading_days_exchange_isolation(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    sse_df = pd.DataFrame({
        "exchange": ["SSE"],
        "cal_date": [date(2024, 1, 2)],
        "is_open": [True],
        "pretrade_date": [date(2023, 12, 29)],
    })
    szse_df = pd.DataFrame({
        "exchange": ["SZSE"],
        "cal_date": [date(2024, 1, 3)],
        "is_open": [True],
        "pretrade_date": [date(2024, 1, 2)],
    })
    write_trade_cal(tmp_path, "SSE", sse_df)
    write_trade_cal(tmp_path, "SZSE", szse_df)
    with MetaStore(db_path) as store:
        store.load_trade_cal_from_parquet(tmp_path)
        sse_days = store.get_trading_days("SSE", date(2024, 1, 1), date(2024, 1, 6))
        szse_days = store.get_trading_days("SZSE", date(2024, 1, 1), date(2024, 1, 6))
    assert sse_days == [date(2024, 1, 2)]
    assert szse_days == [date(2024, 1, 3)]
