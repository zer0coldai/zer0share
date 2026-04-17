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
