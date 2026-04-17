import click
from loguru import logger
from pathlib import Path

from src.config import load_config
from src.fetcher import TushareFetcher
from src.notifier import Notifier
from src.pipeline import Pipeline
from src.storage import MetaStore


def _make_pipeline(config_path: str = "config/settings.toml") -> Pipeline:
    cfg = load_config(Path(config_path))
    cfg.log_path.parent.mkdir(parents=True, exist_ok=True)
    logger.add(cfg.log_path, rotation="10 MB", retention="30 days")
    fetcher = TushareFetcher(cfg.tushare_token)
    notifier = Notifier(cfg.wecom_webhook_url, cfg.notifier_enabled)
    return Pipeline(cfg, fetcher, notifier)


@click.group()
def cli():
    pass


@cli.command()
@click.option("--table", type=click.Choice(["daily_kline", "basic"]), default=None)
@click.option("--all", "sync_all", is_flag=True, default=False)
def sync(table: str | None, sync_all: bool) -> None:
    """增量同步数据"""
    with _make_pipeline() as pipeline:
        if sync_all or table == "basic":
            pipeline.sync_basic()
        if sync_all or table == "daily_kline":
            pipeline.sync_daily_kline()


@cli.command()
def status() -> None:
    """显示各表最后更新时间"""
    cfg = load_config()
    with MetaStore(cfg.db_path) as store:
        for table in ["daily_kline", "basic"]:
            last = store.get_last_date(table)
            click.echo(f"{table}: {last or '从未同步'}")


@cli.command("scheduler")
@click.argument("action", type=click.Choice(["start"]))
def scheduler_cmd(action: str) -> None:
    """启动定时调度"""
    from src.scheduler import start_scheduler
    start_scheduler()
