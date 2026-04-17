from datetime import datetime
from pathlib import Path

import click
from loguru import logger

from src.config import load_config
from src.fetcher import TushareFetcher
from src.notifier import Notifier
from src.pipeline import Pipeline
from src.storage import MetaStore


_logger_initialized = False


def _init_logger(log_path: Path) -> None:
    global _logger_initialized
    if not _logger_initialized:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        logger.add(log_path, rotation="10 MB", retention="30 days")
        _logger_initialized = True


def _make_pipeline(config_path: str = "config/settings.toml") -> Pipeline:
    cfg = load_config(Path(config_path))
    _init_logger(cfg.log_path)
    fetcher = TushareFetcher(cfg.tushare_token)
    notifier = Notifier(cfg.wecom_webhook_url, cfg.notifier_enabled)
    return Pipeline(cfg, fetcher, notifier)


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--table",
    type=click.Choice(["daily_kline", "basic", "trade_cal"]),
    default=None,
)
@click.option("--all", "sync_all", is_flag=True, default=False)
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d"]), default=None)
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]), default=None)
def sync(
    table: str | None,
    sync_all: bool,
    start_date: datetime | None,
    end_date: datetime | None,
) -> None:
    """同步数据。"""
    if end_date is not None and start_date is None:
        raise click.UsageError("--end-date requires --start-date")
    if (start_date is not None or end_date is not None) and table != "daily_kline":
        raise click.UsageError("date range options are only supported for daily_kline")

    parsed_start_date = start_date.date() if start_date is not None else None
    parsed_end_date = end_date.date() if end_date is not None else None
    if (
        parsed_start_date is not None
        and parsed_end_date is not None
        and parsed_end_date < parsed_start_date
    ):
        raise click.UsageError("--end-date must be on or after --start-date")

    with _make_pipeline() as pipeline:
        if sync_all or table == "trade_cal":
            pipeline.sync_trade_cal()
        if sync_all or table == "basic":
            pipeline.sync_basic()
        if sync_all or table == "daily_kline":
            pipeline.sync_daily_kline(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )


@cli.command()
def status() -> None:
    """显示各表最后更新时间。"""
    cfg = load_config(Path("config/settings.toml"))
    with MetaStore(cfg.db_path) as store:
        for table in ["trade_cal", "daily_kline", "basic"]:
            last = store.get_last_date(table)
            click.echo(f"{table}: {last or '从未同步'}")


@cli.command("scheduler")
@click.argument("action", type=click.Choice(["start"]))
def scheduler_cmd(action: str) -> None:
    """启动定时调度。"""
    from src.scheduler import start_scheduler

    start_scheduler()
