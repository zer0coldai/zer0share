from datetime import date
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from src.cli import cli


def test_sync_daily_kline_accepts_date_range():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("src.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "daily_kline",
                "--start-date",
                "2016-01-01",
                "--end-date",
                "2016-01-31",
            ],
        )

    assert result.exit_code == 0
    pipeline.sync_daily_kline.assert_called_once_with(
        start_date=date(2016, 1, 1),
        end_date=date(2016, 1, 31),
    )


def test_sync_end_date_requires_start_date():
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["sync", "--table", "daily_kline", "--end-date", "2016-01-31"],
    )

    assert result.exit_code != 0
    assert "--end-date requires --start-date" in result.output
