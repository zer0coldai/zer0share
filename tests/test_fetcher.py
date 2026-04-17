import pytest
import pandas as pd
from datetime import date
from unittest.mock import patch, MagicMock
from src.fetcher import TushareFetcher


@pytest.fixture
def mock_pro():
    with patch("tushare.pro_api") as mock:
        yield mock.return_value


def test_fetch_basic_returns_correct_columns(mock_pro):
    mock_pro.stock_basic.return_value = pd.DataFrame({
        "ts_code": ["000001.SZ"],
        "symbol": ["000001"],
        "name": ["平安银行"],
        "area": ["深圳"],
        "industry": ["银行"],
        "market": ["主板"],
        "list_status": ["L"],
        "list_date": ["19910403"],
        "delist_date": [None],
    })
    fetcher = TushareFetcher("fake_token")
    df = fetcher.fetch_basic()
    assert list(df.columns) == [
        "ts_code", "symbol", "name", "area", "industry",
        "market", "list_status", "list_date", "delist_date"
    ]
    assert len(df) == 1


def test_fetch_basic_converts_dates(mock_pro):
    mock_pro.stock_basic.return_value = pd.DataFrame({
        "ts_code": ["000001.SZ"], "symbol": ["000001"], "name": ["平安银行"],
        "area": ["深圳"], "industry": ["银行"], "market": ["主板"],
        "list_status": ["L"], "list_date": ["19910403"], "delist_date": [None],
    })
    fetcher = TushareFetcher("fake_token")
    df = fetcher.fetch_basic()
    assert df.iloc[0]["list_date"] == date(1991, 4, 3)
    assert df.iloc[0]["delist_date"] is None


def test_fetch_daily_kline_returns_correct_data(mock_pro):
    mock_pro.daily.return_value = pd.DataFrame({
        "ts_code": ["000001.SZ"],
        "trade_date": ["20240102"],
        "open": [10.0], "high": [11.0], "low": [9.5],
        "close": [10.5], "pre_close": [10.0],
        "change": [0.5], "pct_chg": [5.0],
        "vol": [100000.0], "amount": [1050000.0],
    })
    fetcher = TushareFetcher("fake_token")
    df = fetcher.fetch_daily_kline(date(2024, 1, 2))
    assert len(df) == 1
    assert df.iloc[0]["ts_code"] == "000001.SZ"
    assert df.iloc[0]["trade_date"] == date(2024, 1, 2)


def test_fetch_daily_kline_returns_empty_on_no_data(mock_pro):
    mock_pro.daily.return_value = pd.DataFrame()
    fetcher = TushareFetcher("fake_token")
    df = fetcher.fetch_daily_kline(date(2024, 1, 1))
    assert df.empty


def test_fetch_daily_kline_returns_empty_when_none(mock_pro):
    mock_pro.daily.return_value = None
    fetcher = TushareFetcher("fake_token")
    df = fetcher.fetch_daily_kline(date(2024, 1, 1))
    assert df.empty
