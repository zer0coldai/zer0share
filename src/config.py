from dataclasses import dataclass
from pathlib import Path
import tomllib


@dataclass(frozen=True)
class Config:
    tushare_token: str
    data_dir: Path
    db_path: Path
    log_path: Path
    basic_refresh_days: int
    scheduler_daily_kline_hour: int
    scheduler_daily_kline_minute: int
    scheduler_basic_day_of_week: str
    scheduler_basic_hour: int
    wecom_webhook_url: str
    notifier_enabled: bool


def load_config(path: Path = Path("config/settings.toml")) -> Config:
    with open(path, "rb") as f:
        raw = tomllib.load(f)
    return Config(
        tushare_token=raw["tushare"]["token"],
        data_dir=Path(raw["paths"]["data_dir"]),
        db_path=Path(raw["paths"]["db_path"]),
        log_path=Path(raw["paths"]["log_path"]),
        basic_refresh_days=raw["basic"]["refresh_days"],
        scheduler_daily_kline_hour=raw["scheduler"]["daily_kline_hour"],
        scheduler_daily_kline_minute=raw["scheduler"]["daily_kline_minute"],
        scheduler_basic_day_of_week=raw["scheduler"]["basic_day_of_week"],
        scheduler_basic_hour=raw["scheduler"]["basic_hour"],
        wecom_webhook_url=raw["notifier"]["wecom_webhook_url"],
        notifier_enabled=raw["notifier"]["enabled"],
    )
