from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.pipeline import Pipeline
from src.storage import write_basic


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


@pytest.fixture
def cfg(tmp_path):
    c = MagicMock()
    c.data_dir = tmp_path
    c.db_path = tmp_path / "meta.duckdb"
    c.basic_refresh_days = 7
    return c


@pytest.fixture
def pipeline(cfg):
    fetcher = MagicMock()
    notifier = MagicMock()
    return Pipeline(cfg, fetcher, notifier)


def test_sync_basic_first_run_writes_parquet(pipeline, cfg):
    pipeline._fetcher.fetch_basic.return_value = _basic_df()
    pipeline.sync_basic()
    assert (cfg.data_dir / "basic" / "data.parquet").exists()


def test_sync_basic_skips_if_recently_updated(pipeline):
    pipeline._fetcher.fetch_basic.return_value = _basic_df()
    pipeline.sync_basic()
    pipeline._fetcher.fetch_basic.reset_mock()
    pipeline.sync_basic()
    pipeline._fetcher.fetch_basic.assert_not_called()


def test_sync_daily_kline_writes_parquet(pipeline, cfg):
    write_basic(cfg.data_dir, _basic_df())
    kline_df = pd.DataFrame(
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
    pipeline._fetcher.fetch_daily_kline.return_value = kline_df
    pipeline._meta.update_last_date("daily_kline", date(2024, 1, 1))

    with patch("src.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_daily_kline()

    assert (cfg.data_dir / "daily_kline" / "date=20240102" / "data.parquet").exists()


def test_sync_daily_kline_skips_empty_dates(pipeline, cfg):
    write_basic(cfg.data_dir, _basic_df())
    pipeline._fetcher.fetch_daily_kline.return_value = pd.DataFrame()
    pipeline._meta.update_last_date("daily_kline", date(2024, 1, 1))

    with patch("src.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_daily_kline()

    assert not (cfg.data_dir / "daily_kline" / "date=20240102" / "data.parquet").exists()


def test_sync_daily_kline_sends_completion_notification(pipeline, cfg):
    write_basic(cfg.data_dir, _basic_df())
    kline_df = pd.DataFrame(
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
    pipeline._fetcher.fetch_daily_kline.return_value = kline_df
    pipeline._meta.update_last_date("daily_kline", date(2024, 1, 1))

    with patch("src.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_daily_kline()

    pipeline._notifier.send.assert_called_once()
    msg = pipeline._notifier.send.call_args[0][0]
    assert "成功" in msg


def test_sync_daily_kline_already_up_to_date(pipeline, cfg):
    write_basic(cfg.data_dir, _basic_df())
    today = date.today()
    pipeline._meta.update_last_date("daily_kline", today)
    pipeline.sync_daily_kline()
    pipeline._fetcher.fetch_daily_kline.assert_not_called()


def test_sync_basic_failure_sends_alert_and_raises(pipeline):
    pipeline._fetcher.fetch_basic.side_effect = RuntimeError("API error")
    with pytest.raises(RuntimeError):
        pipeline.sync_basic()
    pipeline._notifier.send.assert_called_once()
    msg = pipeline._notifier.send.call_args[0][0]
    assert "basic 同步失败" in msg


def test_sync_daily_kline_failure_sends_alert_and_raises(pipeline, cfg):
    write_basic(cfg.data_dir, _basic_df())
    pipeline._fetcher.fetch_daily_kline.side_effect = RuntimeError("API error")
    pipeline._meta.update_last_date("daily_kline", date(2024, 1, 1))

    with patch("src.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        with pytest.raises(RuntimeError):
            pipeline.sync_daily_kline()

    pipeline._notifier.send.assert_called_once()
    msg = pipeline._notifier.send.call_args[0][0]
    assert "同步失败" in msg


def test_pipeline_context_manager(cfg):
    fetcher = MagicMock()
    notifier = MagicMock()
    with Pipeline(cfg, fetcher, notifier) as p:
        assert p is not None
