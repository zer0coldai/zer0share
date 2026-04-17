from datetime import date, timedelta
from loguru import logger

from src.config import Config
from src.fetcher import TushareFetcher
from src.notifier import Notifier
from src.storage import MetaStore, write_basic, write_daily_kline, write_trade_cal

FIRST_DATE = date(2010, 1, 4)

EXCHANGES = ["SSE", "SZSE", "CFFEX", "SHFE", "CZCE", "DCE", "INE"]


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

    def sync_daily_kline(self) -> None:
        last = self._meta.get_last_date("daily_kline")
        start = (last + timedelta(days=1)) if last else FIRST_DATE
        today = date.today()

        if start > today:
            logger.info("daily_kline 已是最新，无需同步")
            return

        trading_days = self._meta.get_trading_days("SSE", start, today)
        if not trading_days and self._meta.get_last_date("trade_cal") is None:
            raise RuntimeError(
                "DuckDB 中无 SSE trade_cal 数据，请先运行: "
                "python main.py sync --table trade_cal"
            )

        if not trading_days:
            logger.info("指定范围内无交易日，无需同步")
            return

        success = 0
        for trade_date in trading_days:
            try:
                df = self._fetcher.fetch_daily_kline(trade_date)
                if not df.empty:
                    write_daily_kline(self._cfg.data_dir, trade_date, df)
                    self._meta.update_last_date("daily_kline", trade_date)
                    success += 1
            except Exception as e:
                logger.error(f"daily_kline {trade_date} 同步失败: {e}")
                self._notifier.send(f"daily_kline {trade_date} 同步失败: {e}")
                raise

        msg = f"daily_kline 同步完成: 成功 {success} 天（共 {len(trading_days)} 个交易日）"
        logger.info(msg)
        self._notifier.send(msg)

    def close(self) -> None:
        self._meta.close()

    def __enter__(self) -> "Pipeline":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False
