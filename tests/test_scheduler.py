import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path


VALID_CONFIG = """
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
"""


def test_start_scheduler_registers_two_jobs(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(VALID_CONFIG, encoding="utf-8")

    registered_jobs = []

    def fake_add_job(func, trigger, id=None, **kwargs):
        registered_jobs.append(id)

    with patch("tushare.pro_api"), \
         patch("apscheduler.schedulers.blocking.BlockingScheduler.start"), \
         patch("apscheduler.schedulers.blocking.BlockingScheduler.add_job", side_effect=fake_add_job), \
         patch("src.scheduler.Pipeline") as mock_pipeline_cls:
        mock_pipeline_cls.return_value.__enter__ = lambda s: s
        mock_pipeline_cls.return_value.__exit__ = MagicMock(return_value=False)
        from src.scheduler import start_scheduler
        start_scheduler(str(cfg_file))

    assert set(registered_jobs) == {"daily_kline", "basic"}
    assert len(registered_jobs) == 2
