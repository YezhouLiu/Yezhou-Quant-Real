# =============================================================================
# Yezhou Capital Limited  |  Proprietary & Confidential
# =============================================================================
# Copyright (c) 2026 Yezhou Capital Limited. All rights reserved.
#
# Project  : Yezhou Quantitative Trading System
# Author   : Yezhou Liu
# Contact  : yezhoucapital@gmail.com
#
# This source code is the exclusive property of Yezhou Capital Limited.
# Unauthorized copying, modification, distribution, or use of this file,
# via any medium, is strictly prohibited without prior written consent.
# =============================================================================
import datetime as dt
import pytest
import pandas as pd
from unittest.mock import MagicMock

from factors.decline_streak import calc_single_instrument_decline_streak


@pytest.fixture
def mock_conn():
    return MagicMock()


class CaptureInsert:
    def __init__(self):
        self.rows = None

    def __call__(self, conn, rows):
        self.rows = rows


def _make_df(dates, closes):
    return pd.DataFrame(
        {
            "instrument_id": 1,
            "date": pd.to_datetime(dates),
            "adj_close": closes,
            "open_price": closes,
            "high_price": closes,
            "low_price": closes,
            "close_price": closes,
            "volume": [1_000_000] * len(closes),
            "adj_volume": [1_000_000] * len(closes),
        }
    )


def _fake_get_prices_factory(df):
    def fake_get_prices(conn, instrument_id, start_date=None, end_date=None):
        out = df.copy()
        if start_date:
            out = out[out["date"] >= pd.to_datetime(start_date)]
        if end_date:
            out = out[out["date"] <= pd.to_datetime(end_date)]
        return out.reset_index(drop=True)
    return fake_get_prices


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_three_consecutive_down_days(monkeypatch, mock_conn):
    """After three consecutive declines the streak should equal 3."""
    cap = CaptureInsert()
    monkeypatch.setattr("factors.decline_streak.batch_insert_factor_values", cap)

    # 20 flat days then 3 strictly declining days
    dates = pd.date_range("2021-01-01", periods=23, freq="B")
    closes = [100.0] * 20 + [99.0, 98.0, 97.0]

    df = _make_df(dates, closes)
    monkeypatch.setattr("factors.decline_streak.get_prices", _fake_get_prices_factory(df))

    n = calc_single_instrument_decline_streak(
        mock_conn,
        instrument_id=1,
        start_date=dates[20].date().isoformat(),
        end_date=dates[-1].date().isoformat(),
    )

    assert n > 0, "expected rows to be written"
    assert cap.rows is not None

    streak_values = [int(r["factor_value"]) for r in cap.rows]
    assert streak_values == [1, 2, 3], f"expected [1, 2, 3], got {streak_values}"


def test_streak_resets_on_up_day(monkeypatch, mock_conn):
    """An up day after a run must reset the streak to 0."""
    cap = CaptureInsert()
    monkeypatch.setattr("factors.decline_streak.batch_insert_factor_values", cap)

    # down, down, UP, down
    dates = pd.date_range("2021-03-01", periods=24, freq="B")
    closes = [100.0] * 20 + [99.0, 98.0, 99.5, 98.0]

    df = _make_df(dates, closes)
    monkeypatch.setattr("factors.decline_streak.get_prices", _fake_get_prices_factory(df))

    n = calc_single_instrument_decline_streak(
        mock_conn,
        instrument_id=1,
        start_date=dates[20].date().isoformat(),
        end_date=dates[-1].date().isoformat(),
    )

    assert n > 0
    streak_values = [int(r["factor_value"]) for r in cap.rows]
    # day1: 1, day2: 2, day3(up): 0, day4: 1
    assert streak_values == [1, 2, 0, 1], f"got {streak_values}"


def test_flat_day_resets_streak(monkeypatch, mock_conn):
    """A flat close (equal to previous) must NOT count as a decline."""
    cap = CaptureInsert()
    monkeypatch.setattr("factors.decline_streak.batch_insert_factor_values", cap)

    dates = pd.date_range("2021-06-01", periods=22, freq="B")
    # down, flat, down
    closes = [100.0] * 20 + [99.0, 99.0, 98.0]

    df = _make_df(dates, closes[:22])
    monkeypatch.setattr("factors.decline_streak.get_prices", _fake_get_prices_factory(df))

    n = calc_single_instrument_decline_streak(
        mock_conn,
        instrument_id=1,
        start_date=dates[20].date().isoformat(),
        end_date=dates[21].date().isoformat(),
    )

    assert n > 0
    streak_values = [int(r["factor_value"]) for r in cap.rows]
    # day1 99 < 100 → 1; day2 99 == 99 (not strictly less) → 0
    assert streak_values == [1, 0], f"got {streak_values}"


def test_all_up_days_produces_zeros(monkeypatch, mock_conn):
    """When the stock only rises, every streak value must be 0."""
    cap = CaptureInsert()
    monkeypatch.setattr("factors.decline_streak.batch_insert_factor_values", cap)

    dates = pd.date_range("2021-09-01", periods=30, freq="B")
    closes = [100.0 + i for i in range(30)]  # monotonically rising

    df = _make_df(dates, closes)
    monkeypatch.setattr("factors.decline_streak.get_prices", _fake_get_prices_factory(df))

    n = calc_single_instrument_decline_streak(
        mock_conn,
        instrument_id=1,
        start_date=dates[5].date().isoformat(),
        end_date=dates[-1].date().isoformat(),
    )

    assert n > 0
    for row in cap.rows:
        assert int(row["factor_value"]) == 0, f"expected 0, got {row['factor_value']}"


def test_buffer_loaded_before_start_date(monkeypatch, mock_conn):
    """get_prices must be called with a date well before the target start_date."""
    received = {}

    dates = pd.date_range("2015-01-01", periods=1500, freq="B")
    closes = [100.0 - (i % 5) for i in range(1500)]
    df = _make_df(dates, closes)

    def capturing_get_prices(conn, instrument_id, start_date=None, end_date=None):
        received["start_date"] = start_date
        out = df.copy()
        if start_date:
            out = out[out["date"] >= pd.to_datetime(start_date)]
        if end_date:
            out = out[out["date"] <= pd.to_datetime(end_date)]
        return out.reset_index(drop=True)

    monkeypatch.setattr("factors.decline_streak.get_prices", capturing_get_prices)
    monkeypatch.setattr("factors.decline_streak.batch_insert_factor_values", lambda *a, **k: None)

    target_start = "2021-01-01"
    calc_single_instrument_decline_streak(
        mock_conn,
        instrument_id=1,
        start_date=target_start,
        end_date="2021-06-01",
    )

    assert received.get("start_date") is not None
    assert pd.to_datetime(received["start_date"]).date() < dt.date(2021, 1, 1)


def test_empty_prices_returns_0(monkeypatch, mock_conn):
    monkeypatch.setattr(
        "factors.decline_streak.get_prices",
        lambda *args, **kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(
        "factors.decline_streak.batch_insert_factor_values",
        lambda *args, **kwargs: None,
    )

    n = calc_single_instrument_decline_streak(
        mock_conn,
        instrument_id=1,
        start_date="2021-01-01",
        end_date="2021-06-01",
    )
    assert n == 0


def test_start_after_end_returns_0(monkeypatch, mock_conn):
    monkeypatch.setattr("factors.decline_streak.get_prices", lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr("factors.decline_streak.batch_insert_factor_values", lambda *a, **k: None)

    n = calc_single_instrument_decline_streak(
        mock_conn,
        instrument_id=1,
        start_date="2021-06-01",
        end_date="2021-01-01",
    )
    assert n == 0
