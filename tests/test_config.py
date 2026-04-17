import pytest
from pathlib import Path
from src.config import load_config, Config


VALID_TOML = """
[tushare]
token = "test_token"

[paths]
data_dir = "data"
db_path = "db/meta.duckdb"
log_path = "logs/pipeline.log"

[basic]
refresh_days = 7

[scheduler]
daily_kline_hour = 18
daily_kline_minute = 0
basic_day_of_week = "mon"
basic_hour = 8

[notifier]
wecom_webhook_url = "https://example.com/webhook"
enabled = false
"""


def test_load_config_returns_all_fields(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(VALID_TOML, encoding="utf-8")
    cfg = load_config(cfg_file)
    assert cfg.tushare_token == "test_token"
    assert cfg.data_dir == Path("data")
    assert cfg.db_path == Path("db/meta.duckdb")
    assert cfg.log_path == Path("logs/pipeline.log")
    assert cfg.basic_refresh_days == 7
    assert cfg.scheduler_daily_kline_hour == 18
    assert cfg.scheduler_daily_kline_minute == 0
    assert cfg.scheduler_basic_day_of_week == "mon"
    assert cfg.scheduler_basic_hour == 8
    assert cfg.wecom_webhook_url == "https://example.com/webhook"
    assert cfg.notifier_enabled is False


def test_load_config_notifier_enabled_true(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(VALID_TOML.replace("enabled = false", "enabled = true"), encoding="utf-8")
    cfg = load_config(cfg_file)
    assert cfg.notifier_enabled is True


def test_load_config_file_not_found():
    with pytest.raises(FileNotFoundError, match="配置文件不存在"):
        load_config(Path("nonexistent/settings.toml"))


def test_load_config_missing_key(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text("[tushare]\n# token missing\n[paths]\ndata_dir='data'\ndb_path='db/meta.duckdb'\nlog_path='logs/pipeline.log'\n[basic]\nrefresh_days=7\n[scheduler]\ndaily_kline_hour=18\ndaily_kline_minute=0\nbasic_day_of_week='mon'\nbasic_hour=8\n[notifier]\nwecom_webhook_url='https://x.com'\nenabled=false\n", encoding="utf-8")
    with pytest.raises(KeyError, match="配置文件缺少必要字段"):
        load_config(cfg_file)


def test_config_is_immutable(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(VALID_TOML, encoding="utf-8")
    cfg = load_config(cfg_file)
    with pytest.raises(Exception):
        cfg.tushare_token = "hacked"
