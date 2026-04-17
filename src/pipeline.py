from datetime import date, timedelta
from loguru import logger

from src.config import Config
from src.fetcher import TushareFetcher
from src.notifier import Notifier
from src.storage import MetaStore, write_basic, write_daily_kline

FIRST_DATE = date(2010, 1, 4)


class Pipeline:
    def __init__(self, cfg: Config, fetcher: TushareFetcher, notifier: Notifier):
        self._cfg = cfg
        self._fetcher = fetcher
        self._notifier = notifier
        self._meta = MetaStore(cfg.db_path)

    def sync_basic(self) -> None:
        last = self._meta.get_last_date("basic")
        today = date.today()
        if last and (today - last).days < self._cfg.basic_refresh_days:
            logger.info(f"basic 距上次更新 {(today - last).days} 天，跳过")
            return
        try:
            df = self._fetcher.fetch_basic()
            write_basic(self._cfg.data_dir, df)
            self._meta.update_last_date("basic", today)
            logger.info(f"basic 同步完成: {len(df)} 条")
        except Exception as e:
            logger.error(f"basic 同步失败: {e}")
            self._notifier.send(f"basic 同步失败: {e}")
            raise

    def sync_daily_kline(self) -> None:
        last = self._meta.get_last_date("daily_kline")
        start = (last + timedelta(days=1)) if last else FIRST_DATE
        today = date.today()

        if start > today:
            logger.info("daily_kline 已是最新，无需同步")
            return

        success, skipped = 0, 0
        current = start
        while current <= today:
            try:
                df = self._fetcher.fetch_daily_kline(current)
                if df.empty:
                    skipped += 1
                else:
                    write_daily_kline(self._cfg.data_dir, current, df)
                    self._meta.update_last_date("daily_kline", current)
                    success += 1
            except Exception as e:
                logger.error(f"daily_kline {current} 同步失败: {e}")
                self._notifier.send(f"daily_kline {current} 同步失败: {e}")
                raise
            current += timedelta(days=1)

        msg = f"daily_kline 同步完成: 成功 {success} 天，跳过 {skipped} 天（非交易日）"
        logger.info(msg)
        self._notifier.send(msg)

    def close(self) -> None:
        self._meta.close()

    def __enter__(self) -> "Pipeline":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False
