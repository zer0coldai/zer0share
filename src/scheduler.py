from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from src.config import load_config
from src.fetcher import TushareFetcher
from src.notifier import Notifier
from src.pipeline import Pipeline


def start_scheduler(config_path: str = "config/settings.toml") -> None:
    from pathlib import Path
    cfg = load_config(Path(config_path))
    cfg.log_path.parent.mkdir(parents=True, exist_ok=True)
    logger.add(cfg.log_path, rotation="10 MB", retention="30 days")

    fetcher = TushareFetcher(cfg.tushare_token)
    notifier = Notifier(cfg.wecom_webhook_url, cfg.notifier_enabled)
    pipeline = Pipeline(cfg, fetcher, notifier)

    scheduler = BlockingScheduler()
    scheduler.add_job(
        pipeline.sync_daily_kline,
        CronTrigger(
            hour=cfg.scheduler_daily_kline_hour,
            minute=cfg.scheduler_daily_kline_minute
        ),
        id="daily_kline",
    )
    scheduler.add_job(
        pipeline.sync_basic,
        CronTrigger(
            day_of_week=cfg.scheduler_basic_day_of_week,
            hour=cfg.scheduler_basic_hour
        ),
        id="basic",
    )
    logger.info(
        f"调度器启动: daily_kline 每天 "
        f"{cfg.scheduler_daily_kline_hour}:{cfg.scheduler_daily_kline_minute:02d}, "
        f"basic 每周{cfg.scheduler_basic_day_of_week} {cfg.scheduler_basic_hour}:00"
    )
    scheduler.start()
