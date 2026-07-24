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

from factors.volume_ratio import calc_single_instrument_volume_ratio


@pytest.fixture
def mock_conn():
    return MagicMock()


class CaptureInsert:
    def __init__(self):
        self.rows = None

    def __call__(self, conn, rows):
        self.rows = rows


def _make_df(dates, volumes, prices=None):
    """Helper: build a fake market_prices DataFrame."""
    if prices is None:
        prices = [100.0] * len(dates)
    return pd.DataFrame(
        {
            "instrument_id": 1,
            "date": pd.to_datetime(dates),
            "adj_close": prices,
            "adj_volume": volumes,
            # Extra columns that get_prices returns but volume_ratio doesn't use
            "open_price": prices,
            "high_price": prices,
            "low_price": prices,
            "close_price": prices,
            "volume": volumes,
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

def test_volume_spike_produces_ratio_above_one(monkeypatch, mock_conn):
    """A day with 5x normal volume should produce vol_ratio_20d ≈ 5.0."""
    cap = CaptureInsert()
    monkeypatch.setattr("factors.volume_ratio.batch_insert_factor_values", cap)

    # 30 normal days then 1 spike day
    normal_vol = 1_000_000
    spike_vol = 5_000_000
    dates = pd.date_range("2020-01-01", periods=31, freq="B")
    volumes = [normal_vol] * 30 + [spike_vol]

    df = _make_df(dates, volumes)
    monkeypatch.setattr("factors.volume_ratio.get_prices", _fake_get_prices_factory(df))

    n = calc_single_instrument_volume_ratio(
        mock_conn,
        instrument_id=1,
        start_date=dates[-1].date().isoformat(),
        end_date=dates[-1].date().isoformat(),
        window=20,
    )

    assert n > 0
    assert cap.rows is not None
    spike_row = cap.rows[-1]
    assert spike_row["factor_name"] == "vol_ratio_20d"
    # Spike day volume is 5x normal; ratio should be close to 5.0
    assert abs(spike_row["factor_value"] - 5.0) < 0.5


def test_normal_volume_ratio_near_one(monkeypatch, mock_conn):
    """Stable volume should produce a ratio close to 1.0 for non-spike days."""
    cap = CaptureInsert()
    monkeypatch.setattr("factors.volume_ratio.batch_insert_factor_values", cap)

    normal_vol = 1_000_000
    dates = pd.date_range("2020-01-01", periods=60, freq="B")
    volumes = [normal_vol] * 60

    df = _make_df(dates, volumes)
    monkeypatch.setattr("factors.volume_ratio.get_prices", _fake_get_prices_factory(df))

    # Ask for the last 10 days of results
    n = calc_single_instrument_volume_ratio(
        mock_conn,
        instrument_id=1,
        start_date=dates[40].date().isoformat(),
        end_date=dates[-1].date().isoformat(),
        window=20,
    )

    assert n > 0
    for row in cap.rows:
        assert 0.8 < row["factor_value"] < 1.2, f"Expected ratio ≈ 1.0, got {row['factor_value']}"


def test_buffer_loaded_before_start_date(monkeypatch, mock_conn):
    """get_prices must be called with a start date earlier than the requested start."""
    received = {}

    dates = pd.date_range("2018-01-01", periods=300, freq="B")
    df = _make_df(dates, [1_000_000] * 300)

    def capturing_get_prices(conn, instrument_id, start_date=None, end_date=None):
        received["start_date"] = start_date
        out = df.copy()
        if start_date:
            out = out[out["date"] >= pd.to_datetime(start_date)]
        if end_date:
            out = out[out["date"] <= pd.to_datetime(end_date)]
        return out.reset_index(drop=True)

    monkeypatch.setattr("factors.volume_ratio.get_prices", capturing_get_prices)
    monkeypatch.setattr("factors.volume_ratio.batch_insert_factor_values", lambda *a, **k: None)

    target_start = "2019-01-01"
    calc_single_instrument_volume_ratio(
        mock_conn,
        instrument_id=1,
        start_date=target_start,
        end_date="2019-06-01",
        window=20,
    )

    assert received.get("start_date") is not None
    assert pd.to_datetime(received["start_date"]).date() < dt.date(2019, 1, 1)


def test_empty_prices_returns_0(monkeypatch, mock_conn):
    monkeypatch.setattr(
        "factors.volume_ratio.get_prices",
        lambda *args, **kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(
        "factors.volume_ratio.batch_insert_factor_values",
        lambda *args, **kwargs: None,
    )

    n = calc_single_instrument_volume_ratio(
        mock_conn,
        instrument_id=1,
        start_date="2020-01-01",
        end_date="2020-06-01",
        window=20,
    )
    assert n == 0


def test_zero_volume_rows_excluded(monkeypatch, mock_conn):
    """Rows with zero volume must not produce factor values."""
    cap = CaptureInsert()
    monkeypatch.setattr("factors.volume_ratio.batch_insert_factor_values", cap)

    dates = pd.date_range("2020-01-01", periods=40, freq="B")
    # All volume = 0 → no valid ratio
    df = _make_df(dates, [0] * 40)
    monkeypatch.setattr("factors.volume_ratio.get_prices", _fake_get_prices_factory(df))

    n = calc_single_instrument_volume_ratio(
        mock_conn,
        instrument_id=1,
        start_date=dates[25].date().isoformat(),
        end_date=dates[-1].date().isoformat(),
        window=20,
    )
    assert n == 0


def test_start_after_end_returns_0(monkeypatch, mock_conn):
    monkeypatch.setattr("factors.volume_ratio.get_prices", lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr("factors.volume_ratio.batch_insert_factor_values", lambda *a, **k: None)

    n = calc_single_instrument_volume_ratio(
        mock_conn,
        instrument_id=1,
        start_date="2021-06-01",
        end_date="2021-01-01",
        window=20,
    )
    assert n == 0
