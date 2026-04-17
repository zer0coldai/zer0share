import pytest
from pathlib import Path
from datetime import date
from src.storage import MetaStore


def test_init_creates_table(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    store = MetaStore(db_path)
    assert store.get_last_date("daily_kline") is None


def test_update_and_get_last_date(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    store = MetaStore(db_path)
    store.update_last_date("daily_kline", date(2024, 1, 15))
    assert store.get_last_date("daily_kline") == date(2024, 1, 15)


def test_update_overwrites_previous(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    store = MetaStore(db_path)
    store.update_last_date("daily_kline", date(2024, 1, 1))
    store.update_last_date("daily_kline", date(2024, 1, 31))
    assert store.get_last_date("daily_kline") == date(2024, 1, 31)


def test_different_table_names_are_independent(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    store = MetaStore(db_path)
    store.update_last_date("daily_kline", date(2024, 1, 10))
    store.update_last_date("basic", date(2024, 2, 20))
    assert store.get_last_date("daily_kline") == date(2024, 1, 10)
    assert store.get_last_date("basic") == date(2024, 2, 20)
