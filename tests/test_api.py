from datetime import date

import pandas as pd
import pytest

from zer0share.api import LocalPro
from zer0share.storage import write_adj_factor, write_basic, write_daily_kline, write_trade_cal


def test_stock_basic_filters_and_formats_dates(tmp_path):
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "600000.SH"],
            "symbol": ["000001", "600000"],
            "name": ["Ping An Bank", "SPDB"],
            "area": ["Shenzhen", "Shanghai"],
            "industry": ["Bank", "Bank"],
            "fullname": ["Ping An Bank Co., Ltd.", "Shanghai Pudong Development Bank"],
            "enname": ["Ping An Bank", "SPDB"],
            "cnspell": ["payh", "pfyh"],
            "market": ["Main Board", "Main Board"],
            "exchange": ["SZSE", "SSE"],
            "curr_type": ["CNY", "CNY"],
            "list_status": ["L", "L"],
            "list_date": [date(1991, 4, 3), date(1999, 11, 10)],
            "delist_date": [None, None],
            "is_hs": ["S", "H"],
            "act_name": ["Shenzhen Investment Holdings", "Shanghai SASAC"],
            "act_ent_type": ["Local SOE", "Local SOE"],
        }
    )
    write_basic(tmp_path, df)

    pro = LocalPro(tmp_path)
    result = pro.stock_basic(
        ts_code="000001.SZ",
        fields="ts_code,name,list_date,delist_date",
    )

    assert result.to_dict("records") == [
        {
            "ts_code": "000001.SZ",
            "name": "Ping An Bank",
            "list_date": "19910403",
            "delist_date": None,
        }
    ]


def test_trade_cal_filters_open_days_and_formats_dates(tmp_path):
    df = pd.DataFrame(
        {
            "exchange": ["SSE", "SSE", "SSE"],
            "cal_date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "is_open": [False, True, True],
            "pretrade_date": [date(2023, 12, 29), date(2023, 12, 29), date(2024, 1, 2)],
        }
    )
    write_trade_cal(tmp_path, "SSE", df)

    pro = LocalPro(tmp_path)
    result = pro.trade_cal(
        exchange="SSE",
        start_date="2024-01-02",
        end_date="20240103",
        is_open="1",
        fields=["exchange", "cal_date", "is_open", "pretrade_date"],
    )

    assert result.to_dict("records") == [
        {
            "exchange": "SSE",
            "cal_date": "20240102",
            "is_open": True,
            "pretrade_date": "20231229",
        },
        {
            "exchange": "SSE",
            "cal_date": "20240103",
            "is_open": True,
            "pretrade_date": "20240102",
        },
    ]


def test_daily_filters_multiple_codes_by_date_range_and_formats_dates(tmp_path):
    write_daily_kline(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "600000.SH"],
                "trade_date": [date(2024, 1, 2), date(2024, 1, 2)],
                "open": [10.0, 20.0],
                "high": [11.0, 21.0],
                "low": [9.0, 19.0],
                "close": [10.5, 20.5],
                "pre_close": [10.0, 20.0],
                "change": [0.5, 0.5],
                "pct_chg": [5.0, 2.5],
                "vol": [1000.0, 2000.0],
                "amount": [10000.0, 20000.0],
            }
        ),
    )
    write_daily_kline(
        tmp_path,
        date(2024, 1, 3),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "600000.SH"],
                "trade_date": [date(2024, 1, 3), date(2024, 1, 3)],
                "open": [11.0, 21.0],
                "high": [12.0, 22.0],
                "low": [10.0, 20.0],
                "close": [11.5, 21.5],
                "pre_close": [10.5, 20.5],
                "change": [1.0, 1.0],
                "pct_chg": [9.5, 4.9],
                "vol": [1100.0, 2100.0],
                "amount": [11000.0, 21000.0],
            }
        ),
    )

    pro = LocalPro(tmp_path)
    result = pro.daily(
        ts_code="600000.SH,000001.SZ",
        start_date="20240103",
        end_date="20240103",
        fields=["ts_code", "trade_date", "close"],
    )

    assert result.to_dict("records") == [
        {"ts_code": "000001.SZ", "trade_date": "20240103", "close": 11.5},
        {"ts_code": "600000.SH", "trade_date": "20240103", "close": 21.5},
    ]


def test_adj_factor_filters_trade_date_and_formats_dates(tmp_path):
    write_adj_factor(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "600000.SH"],
                "trade_date": [date(2024, 1, 2), date(2024, 1, 2)],
                "adj_factor": [100.1, 200.2],
            }
        ),
    )

    pro = LocalPro(tmp_path)
    result = pro.adj_factor(trade_date="20240102", fields="ts_code,trade_date,adj_factor")

    assert result.to_dict("records") == [
        {"ts_code": "000001.SZ", "trade_date": "20240102", "adj_factor": 100.1},
        {"ts_code": "600000.SH", "trade_date": "20240102", "adj_factor": 200.2},
    ]


def test_daily_rejects_ambiguous_trade_date_and_range(tmp_path):
    pro = LocalPro(tmp_path)

    with pytest.raises(ValueError, match="trade_date"):
        pro.daily(trade_date="20240102", start_date="20240101")


def test_query_dispatches_to_named_api(tmp_path):
    write_adj_factor(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 2)],
                "adj_factor": [100.1],
            }
        ),
    )

    pro = LocalPro(tmp_path)
    result = pro.query("adj_factor", ts_code="000001.SZ")

    assert result.to_dict("records") == [
        {"ts_code": "000001.SZ", "trade_date": "20240102", "adj_factor": 100.1}
    ]


def test_unknown_query_api_raises_value_error(tmp_path):
    pro = LocalPro(tmp_path)

    with pytest.raises(ValueError, match="unknown api"):
        pro.query("moneyflow")


def test_unknown_field_raises_value_error(tmp_path):
    write_basic(
        tmp_path,
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "symbol": ["000001"],
                "name": ["Ping An Bank"],
                "area": ["Shenzhen"],
                "industry": ["Bank"],
                "fullname": ["Ping An Bank Co., Ltd."],
                "enname": ["Ping An Bank"],
                "cnspell": ["payh"],
                "market": ["Main Board"],
                "exchange": ["SZSE"],
                "curr_type": ["CNY"],
                "list_status": ["L"],
                "list_date": [date(1991, 4, 3)],
                "delist_date": [None],
                "is_hs": ["S"],
                "act_name": ["Shenzhen Investment Holdings"],
                "act_ent_type": ["Local SOE"],
            }
        ),
    )
    pro = LocalPro(tmp_path)

    with pytest.raises(ValueError, match="unknown fields"):
        pro.stock_basic(fields="ts_code,not_a_field")


def test_missing_data_raises_file_not_found_with_sync_hint(tmp_path):
    pro = LocalPro(tmp_path)

    with pytest.raises(FileNotFoundError, match="sync --table basic"):
        pro.stock_basic()


def test_invalid_date_format_raises_value_error(tmp_path):
    write_trade_cal(
        tmp_path,
        "SSE",
        pd.DataFrame(
            {
                "exchange": ["SSE"],
                "cal_date": [date(2024, 1, 2)],
                "is_open": [True],
                "pretrade_date": [date(2023, 12, 29)],
            }
        ),
    )
    pro = LocalPro(tmp_path)

    with pytest.raises(ValueError, match="invalid date"):
        pro.trade_cal(start_date="2024/01/02")


def test_invalid_date_range_raises_value_error(tmp_path):
    write_daily_kline(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 2)],
                "open": [10.0],
                "high": [11.0],
                "low": [9.0],
                "close": [10.5],
                "pre_close": [10.0],
                "change": [0.5],
                "pct_chg": [5.0],
                "vol": [1000.0],
                "amount": [10000.0],
            }
        ),
    )
    pro = LocalPro(tmp_path)

    with pytest.raises(ValueError, match="start_date"):
        pro.daily(start_date="20240103", end_date="20240102")


def test_trade_cal_invalid_date_range_raises_value_error(tmp_path):
    write_trade_cal(
        tmp_path,
        "SSE",
        pd.DataFrame(
            {
                "exchange": ["SSE"],
                "cal_date": [date(2024, 1, 2)],
                "is_open": [True],
                "pretrade_date": [date(2023, 12, 29)],
            }
        ),
    )
    pro = LocalPro(tmp_path)

    with pytest.raises(ValueError, match="start_date"):
        pro.trade_cal(start_date="20240103", end_date="20240102")


def test_pro_bar_returns_qfq_prices_using_end_date_factor(tmp_path):
    write_daily_kline(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 2)],
                "open": [10.0],
                "high": [12.0],
                "low": [9.0],
                "close": [11.0],
                "pre_close": [10.0],
                "change": [1.0],
                "pct_chg": [10.0],
                "vol": [1000.0],
                "amount": [11000.0],
            }
        ),
    )
    write_daily_kline(
        tmp_path,
        date(2024, 1, 3),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 3)],
                "open": [20.0],
                "high": [22.0],
                "low": [19.0],
                "close": [21.0],
                "pre_close": [11.0],
                "change": [10.0],
                "pct_chg": [90.91],
                "vol": [2000.0],
                "amount": [42000.0],
            }
        ),
    )
    write_adj_factor(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 2)],
                "adj_factor": [2.0],
            }
        ),
    )
    write_adj_factor(
        tmp_path,
        date(2024, 1, 3),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 3)],
                "adj_factor": [4.0],
            }
        ),
    )

    pro = LocalPro(tmp_path)
    result = pro.pro_bar(
        ts_code="000001.SZ",
        start_date="20240102",
        end_date="20240103",
        adj="qfq",
    )

    assert result[
        ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol"]
    ].to_dict(
        "records"
    ) == [
        {
            "ts_code": "000001.SZ",
            "trade_date": "20240102",
            "open": 5.0,
            "high": 6.0,
            "low": 4.5,
            "close": 5.5,
            "pre_close": 5.0,
            "change": 0.5,
            "pct_chg": 10.0,
            "vol": 1000.0,
        },
        {
            "ts_code": "000001.SZ",
            "trade_date": "20240103",
            "open": 20.0,
            "high": 22.0,
            "low": 19.0,
            "close": 21.0,
            "pre_close": 11.0,
            "change": 10.0,
            "pct_chg": 90.91,
            "vol": 2000.0,
        },
    ]


def test_pro_bar_returns_hfq_prices(tmp_path):
    write_daily_kline(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 2)],
                "open": [10.0],
                "high": [12.0],
                "low": [9.0],
                "close": [11.0],
                "pre_close": [10.0],
                "change": [1.0],
                "pct_chg": [10.0],
                "vol": [1000.0],
                "amount": [11000.0],
            }
        ),
    )
    write_adj_factor(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 2)],
                "adj_factor": [2.0],
            }
        ),
    )

    pro = LocalPro(tmp_path)
    result = pro.pro_bar(ts_code="000001.SZ", trade_date="20240102", adj="hfq")

    assert result[["open", "high", "low", "close", "pre_close"]].to_dict("records") == [
        {"open": 20.0, "high": 24.0, "low": 18.0, "close": 22.0, "pre_close": 20.0}
    ]


def test_pro_bar_rounds_adjusted_prices_to_two_decimals(tmp_path):
    write_daily_kline(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 2)],
                "open": [10.0],
                "high": [10.0],
                "low": [10.0],
                "close": [10.0],
                "pre_close": [10.0],
                "change": [0.0],
                "pct_chg": [0.0],
                "vol": [1000.0],
                "amount": [10000.0],
            }
        ),
    )
    write_adj_factor(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 2)],
                "adj_factor": [1.234],
            }
        ),
    )

    pro = LocalPro(tmp_path)
    result = pro.pro_bar(ts_code="000001.SZ", trade_date="20240102", adj="hfq")

    assert result.iloc[0]["close"] == 12.34


def test_pro_bar_rejects_unsupported_asset_and_freq(tmp_path):
    pro = LocalPro(tmp_path)

    with pytest.raises(NotImplementedError, match="asset='E'"):
        pro.pro_bar(ts_code="000001.SZ", asset="I")

    with pytest.raises(NotImplementedError, match="freq='D'"):
        pro.pro_bar(ts_code="000001.SZ", freq="W")
