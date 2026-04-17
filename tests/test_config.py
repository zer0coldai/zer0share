import pytest
from pathlib import Path
from src.config import load_config, Config

def test_load_config(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text("""
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
""")
    cfg = load_config(cfg_file)
    assert cfg.tushare_token == "test_token"
    assert cfg.data_dir == Path("data")
    assert cfg.basic_refresh_days == 7
    assert cfg.notifier_enabled is False
