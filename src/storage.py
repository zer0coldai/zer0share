import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, datetime, timezone
from pathlib import Path


class MetaStore:
    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(db_path))
        self._init_schema()

    def _init_schema(self):
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS sync_meta (
                table_name  VARCHAR PRIMARY KEY,
                last_date   DATE,
                updated_at  TIMESTAMP
            )
        """)

    def get_last_date(self, table_name: str) -> date | None:
        row = self._conn.execute(
            "SELECT last_date FROM sync_meta WHERE table_name = ?",
            [table_name]
        ).fetchone()
        return row[0] if row else None

    def update_last_date(self, table_name: str, last_date: date):
        self._conn.execute("""
            INSERT INTO sync_meta (table_name, last_date, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT (table_name) DO UPDATE SET
                last_date = excluded.last_date,
                updated_at = excluded.updated_at
        """, [table_name, last_date, datetime.now(timezone.utc)])

    def __enter__(self) -> "MetaStore":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False

    def close(self):
        self._conn.close()


def write_daily_kline(data_dir: Path, trade_date: date, df: pd.DataFrame) -> None:
    partition_dir = data_dir / "daily_kline" / f"date={trade_date.strftime('%Y%m%d')}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, partition_dir / "data.parquet")


def read_daily_kline(data_dir: Path, trade_date: date) -> pd.DataFrame:
    path = data_dir / "daily_kline" / f"date={trade_date.strftime('%Y%m%d')}" / "data.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pq.read_table(path).to_pandas()


def write_basic(data_dir: Path, df: pd.DataFrame) -> None:
    basic_dir = data_dir / "basic"
    basic_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, basic_dir / "data.parquet")


def read_basic(data_dir: Path) -> pd.DataFrame:
    path = data_dir / "basic" / "data.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pq.read_table(path).to_pandas()
