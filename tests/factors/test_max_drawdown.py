import datetime as dt
import pandas as pd
import pytest
from unittest.mock import MagicMock

from factors.max_drawdown import calc_single_instrument_max_drawdown


@pytest.fixture
def mock_conn():
    return MagicMock()


class CaptureInsert:
    def __init__(self):
        self.rows = None

    def __call__(self, conn, rows):
        self.rows = rows


def test_roll_start_has_no_value_but_later_has_values(monkeypatch, mock_conn):
    cap = CaptureInsert()
    monkeypatch.setattr("factors.max_drawdown.batch_insert_factor_values", cap)

    start = dt.date(2004, 1, 1)
    end = dt.date(2017, 1, 10)
    dates = pd.date_range(start, end, freq="D")

    # 人为制造一次大回撤
    prices = []
    p = 100.0
    for i in range(len(dates)):
        if 500 < i < 600:
            p *= 0.99  # 连续下跌
        else:
            p *= 1.001
        prices.append(p)

    df = pd.DataFrame(
        {
            "instrument_id": 1,
            "date": dates,
            "adj_close": prices,
        }
    )

    def fake_get_prices(conn, instrument_id, start_date=None, end_date=None):
        out = df.copy()
        if start_date:
            out = out[out["date"] >= pd.to_datetime(start_date)]
        if end_date:
            out = out[out["date"] <= pd.to_datetime(end_date)]
        return out.reset_index(drop=True)

    monkeypatch.setattr("factors.max_drawdown.get_prices", fake_get_prices)

    n = calc_single_instrument_max_drawdown(
        mock_conn,
        instrument_id=1,
        start_date="2005-01-01",
        end_date="2016-01-01",
        window=252,
        factor_version="v1",
    )

    assert n > 0
    assert cap.rows is not None
    assert min(r["date"] for r in cap.rows) >= "2005-01-01"
    assert max(r["date"] for r in cap.rows) <= "2016-01-01"


def test_buffer_is_required(monkeypatch, mock_conn):
    cap = CaptureInsert()
    monkeypatch.setattr("factors.max_drawdown.batch_insert_factor_values", cap)

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

    monkeypatch.setattr("factors.max_drawdown.get_prices", fake_get_prices)

    _ = calc_single_instrument_max_drawdown(
        mock_conn,
        instrument_id=1,
        start_date="2005-01-01",
        end_date="2006-01-01",
        window=252,
    )

    assert received["start_date"] is not None
    assert pd.to_datetime(received["start_date"]).date() < dt.date(2005, 1, 1)


def test_empty_prices_returns_0(monkeypatch, mock_conn):
    monkeypatch.setattr("factors.max_drawdown.get_prices", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr("factors.max_drawdown.batch_insert_factor_values", lambda *args, **kwargs: None)

    n = calc_single_instrument_max_drawdown(
        mock_conn,
        instrument_id=1,
        start_date="2005-01-01",
        end_date="2006-01-01",
        window=252,
    )
    assert n == 0
