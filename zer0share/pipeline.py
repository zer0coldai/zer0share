from datetime import date, timedelta
import time

from loguru import logger

from zer0share.config import Config
from zer0share.fetcher import TushareFetcher
from zer0share.notifier import Notifier
from zer0share.storage import (
    MetaStore,
    adj_factor_partition_exists,
    daily_kline_partition_exists,
    write_adj_factor,
    write_basic,
    write_daily_kline,
    write_trade_cal,
)


FIRST_DATE = date(2016, 1, 1)
EXCHANGES = ["SSE", "SZSE", "CFFEX", "SHFE", "CZCE", "DCE", "INE"]


class Pipeline:
    def __init__(self, cfg: Config, fetcher: TushareFetcher, notifier: Notifier):
        self._cfg = cfg
        self._fetcher = fetcher
        self._notifier = notifier
        self._meta = MetaStore(cfg.db_path)

    def sync_basic(self) -> None:
        today = date.today()
        try:
            df = self._fetcher.fetch_basic()
            write_basic(self._cfg.data_dir, df)
            self._meta.update_last_date("basic", today)
            logger.info(f"basic 同步完成: {len(df)} 条")
        except Exception as e:
            logger.error(f"basic 同步失败: {e}")
            self._notifier.send(f"basic 同步失败: {e}")
            raise

    def sync_trade_cal(self) -> None:
        try:
            for exchange in EXCHANGES:
                df = self._fetcher.fetch_trade_cal(exchange)
                write_trade_cal(self._cfg.data_dir, exchange, df)
                logger.info(f"trade_cal {exchange} 写入完成: {len(df)} 条")
            self._meta.load_trade_cal_from_parquet(self._cfg.data_dir)
            self._meta.update_last_date("trade_cal", date.today())
            logger.info("trade_cal 全部同步完成")
        except Exception as e:
            logger.error(f"trade_cal 同步失败: {e}")
            self._notifier.send(f"trade_cal 同步失败: {e}")
            raise

    def sync_daily_kline(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        today = date.today()
        last = self._meta.get_last_date("daily_kline")

        if start_date is None:
            start = (last + timedelta(days=1)) if last else FIRST_DATE
            end = today
        else:
            start = start_date
            end = end_date or today

        if start_date is None and start > end:
            logger.info("daily_kline 已是最新，无需同步")
            return

        if start > end:
            raise ValueError("start_date must be on or before end_date")

        trading_days = self._meta.get_trading_days("SSE", start, end)
        if not trading_days and self._meta.get_last_date("trade_cal") is None:
            raise RuntimeError(
                "DuckDB 中无 SSE trade_cal 数据，请先运行 "
                "python main.py sync --table trade_cal"
            )

        if not trading_days:
            logger.info("指定范围内无交易日，无需同步")
            return

        success = 0
        skipped_existing = 0
        frontier = last

        for trade_date in trading_days:
            if daily_kline_partition_exists(self._cfg.data_dir, trade_date):
                skipped_existing += 1
                continue
            try:
                df = self._fetcher.fetch_daily_kline(trade_date)
                time.sleep(0.2)
                if not df.empty:
                    write_daily_kline(self._cfg.data_dir, trade_date, df)
                    if frontier is None or trade_date > frontier:
                        self._meta.update_last_date("daily_kline", trade_date)
                        frontier = trade_date
                    success += 1
            except Exception as e:
                logger.error(f"daily_kline {trade_date} 同步失败: {e}")
                self._notifier.send(f"daily_kline {trade_date} 同步失败: {e}")
                raise

        msg = (
            f"daily_kline 同步完成: 成功 {success} 天, "
            f"跳过已存在 {skipped_existing} 天, 共 {len(trading_days)} 个交易日"
        )
        logger.info(msg)
        self._notifier.send(msg)

    def sync_adj_factor(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        today = date.today()
        last = self._meta.get_last_date("adj_factor")

        if start_date is None:
            start = (last + timedelta(days=1)) if last else FIRST_DATE
            end = today
        else:
            start = start_date
            end = end_date or today

        if start_date is None and start > end:
            logger.info("adj_factor 已是最新，无需同步")
            return

        if start > end:
            raise ValueError("start_date must be on or before end_date")

        trading_days = self._meta.get_trading_days("SSE", start, end)
        if not trading_days and self._meta.get_last_date("trade_cal") is None:
            raise RuntimeError(
                "DuckDB 中无 SSE trade_cal 数据，请先运行 "
                "python main.py sync --table trade_cal"
            )

        if not trading_days:
            logger.info("指定范围内无交易日，无需同步")
            return

        success = 0
        skipped_existing = 0
        frontier = last

        for trade_date in trading_days:
            if adj_factor_partition_exists(self._cfg.data_dir, trade_date):
                skipped_existing += 1
                continue
            try:
                df = self._fetcher.fetch_adj_factor(trade_date)
                time.sleep(0.2)
                if not df.empty:
                    write_adj_factor(self._cfg.data_dir, trade_date, df)
                    if frontier is None or trade_date > frontier:
                        self._meta.update_last_date("adj_factor", trade_date)
                        frontier = trade_date
                    success += 1
            except Exception as e:
                logger.error(f"adj_factor {trade_date} 同步失败: {e}")
                self._notifier.send(f"adj_factor {trade_date} 同步失败: {e}")
                raise

        msg = (
            f"adj_factor 同步完成: 成功 {success} 天, "
            f"跳过已存在 {skipped_existing} 天, 共 {len(trading_days)} 个交易日"
        )
        logger.info(msg)
        self._notifier.send(msg)

    def close(self) -> None:
        self._meta.close()

    def __enter__(self) -> "Pipeline":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False
