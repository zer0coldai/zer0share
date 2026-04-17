import pytest
from pathlib import Path
from datetime import date
from src.storage import MetaStore


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


import pandas as pd
from src.storage import write_daily_kline, read_daily_kline, write_basic, read_basic


def test_write_and_read_daily_kline(tmp_path):
    df = pd.DataFrame({
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
    })
    write_daily_kline(tmp_path, date(2024, 1, 2), df)
    result = read_daily_kline(tmp_path, date(2024, 1, 2))
    assert len(result) == 2
    assert set(result["ts_code"]) == {"000001.SZ", "000002.SZ"}


def test_daily_kline_partition_path(tmp_path):
    df = pd.DataFrame({
        "ts_code": ["000001.SZ"],
        "trade_date": [date(2024, 1, 2)],
        "open": [10.0], "high": [11.0], "low": [9.5],
        "close": [10.5], "pre_close": [10.0],
        "change": [0.5], "pct_chg": [5.0],
        "vol": [100000.0], "amount": [1050000.0],
    })
    write_daily_kline(tmp_path, date(2024, 1, 2), df)
    assert (tmp_path / "daily_kline" / "date=20240102" / "data.parquet").exists()


def test_write_and_read_basic(tmp_path):
    df = pd.DataFrame({
        "ts_code": ["000001.SZ"],
        "symbol": ["000001"],
        "name": ["平安银行"],
        "area": ["深圳"],
        "industry": ["银行"],
        "market": ["主板"],
        "list_status": ["L"],
        "list_date": [date(1991, 4, 3)],
        "delist_date": [None],
    })
    write_basic(tmp_path, df)
    result = read_basic(tmp_path)
    assert len(result) == 1
    assert result.iloc[0]["name"] == "平安银行"


def test_basic_overwrites_on_second_write(tmp_path):
    df1 = pd.DataFrame({
        "ts_code": ["000001.SZ"], "symbol": ["000001"], "name": ["平安银行"],
        "area": ["深圳"], "industry": ["银行"], "market": ["主板"],
        "list_status": ["L"], "list_date": [date(1991, 4, 3)], "delist_date": [None],
    })
    df2 = pd.DataFrame({
        "ts_code": ["000001.SZ", "000002.SZ"],
        "symbol": ["000001", "000002"], "name": ["平安银行", "万科A"],
        "area": ["深圳", "深圳"], "industry": ["银行", "房地产"], "market": ["主板", "主板"],
        "list_status": ["L", "L"],
        "list_date": [date(1991, 4, 3), date(1991, 1, 29)], "delist_date": [None, None],
    })
    write_basic(tmp_path, df1)
    write_basic(tmp_path, df2)
    result = read_basic(tmp_path)
    assert len(result) == 2
