import datetime as dt
import pandas as pd
import pytest
from unittest.mock import MagicMock

from factors.volatility import calc_single_instrument_volatility


@pytest.fixture
def mock_conn():
    return MagicMock()


class CaptureInsert:
    def __init__(self):
        self.rows = None

    def __call__(self, conn, rows):
        self.rows = rows


def test_roll_start_has_no_value_but_later_has_values(monkeypatch, mock_conn):
    """
    关键语义：
    - start_date 很早，前 window 天 vol 没值很正常
    - 但到了后面必须能写出 vol
    """
    cap = CaptureInsert()
    monkeypatch.setattr("factors.volatility.batch_insert_factor_values", cap)

    # 构造足够长的价格序列（自然日模拟即可）
    start = dt.date(2004, 1, 1)
    end = dt.date(2017, 1, 10)
    dates = pd.date_range(start, end, freq="D")

    # 单调递增价格即可（vol 不会是 0，因为 log-return 不是常数）
    df = pd.DataFrame(
        {
            "instrument_id": 1,
            "date": dates,
            "adj_close": [100.0 + i * 0.01 for i in range(len(dates))],
        }
    )

    def fake_get_prices(conn, instrument_id, start_date=None, end_date=None):
        out = df.copy()
        if start_date:
            out = out[out["date"] >= pd.to_datetime(start_date)]
        if end_date:
            out = out[out["date"] <= pd.to_datetime(end_date)]
        return out.reset_index(drop=True)

    monkeypatch.setattr("factors.volatility.get_prices", fake_get_prices)

    n = calc_single_instrument_volatility(
        mock_conn,
        instrument_id=1,
        start_date="2005-01-01",
        end_date="2016-01-01",
        window=60,
        annualize=252,
        factor_version="v1",
    )

    assert n > 0
    assert cap.rows is not None
    # 写入日期必须落在写表区间内
    assert min(r["date"] for r in cap.rows) >= "2005-01-01"
    assert max(r["date"] for r in cap.rows) <= "2016-01-01"


def test_buffer_is_required(monkeypatch, mock_conn):
    """
    如果不取 start_date 之前的历史，rolling(window) 会被截断。
    本 test 断言 fake_get_prices 收到的 start_date 早于写表 start_date。
    """
    cap = CaptureInsert()
    monkeypatch.setattr("factors.volatility.batch_insert_factor_values", cap)

    received = {"start_date": None, "end_date": None}

    dates = pd.date_range(dt.date(2004, 1, 1), dt.date(2006, 12, 31), freq="D")
    df = pd.DataFrame(
        {
            "instrument_id": 1,
            "date": dates,
            "adj_close": [100.0 + i * 0.01 for i in range(len(dates))],
        }
    )

    def fake_get_prices(conn, instrument_id, start_date=None, end_date=None):
        received["start_date"] = start_date
        received["end_date"] = end_date
        out = df.copy()
        if start_date:
            out = out[out["date"] >= pd.to_datetime(start_date)]
        if end_date:
            out = out[out["date"] <= pd.to_datetime(end_date)]
        return out.reset_index(drop=True)

    monkeypatch.setattr("factors.volatility.get_prices", fake_get_prices)

    _ = calc_single_instrument_volatility(
        mock_conn,
        instrument_id=1,
        start_date="2005-01-01",
        end_date="2006-01-01",
        window=60,
        annualize=252,
        factor_version="v1",
    )

    assert received["start_date"] is not None
    assert pd.to_datetime(received["start_date"]).date() < dt.date(2005, 1, 1)


def test_empty_prices_returns_0(monkeypatch, mock_conn):
    monkeypatch.setattr("factors.volatility.get_prices", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr("factors.volatility.batch_insert_factor_values", lambda *args, **kwargs: None)

    n = calc_single_instrument_volatility(
        mock_conn,
        instrument_id=1,
        start_date="2005-01-01",
        end_date="2006-01-01",
        window=60,
        annualize=252,
        factor_version="v1",
    )
    assert n == 0
