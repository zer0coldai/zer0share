import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path


def test_start_scheduler_registers_two_jobs(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text("""
[tushare]
token = "test"

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
wecom_webhook_url = "https://example.com"
enabled = false
""", encoding="utf-8")

    with patch("tushare.pro_api"), \
         patch("apscheduler.schedulers.blocking.BlockingScheduler.start") as mock_start, \
         patch("src.scheduler.Pipeline"):
        from src.scheduler import start_scheduler
        start_scheduler(str(cfg_file))
        mock_start.assert_called_once()
