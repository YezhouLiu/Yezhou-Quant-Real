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
"""
Tests for reports/daily_briefing.py

Strategy: monkeypatch the two internal helpers that hit the DB
(_load_factor_snapshot and _load_price_snapshot) so tests run without a
real database connection.
"""
import math
import pytest
import pandas as pd
from unittest.mock import MagicMock

from reports.daily_briefing import generate_briefing, format_briefing


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def _make_wide(n_instruments: int = 30) -> pd.DataFrame:
    """Synthetic wide factor DataFrame (one row per ticker)."""
    import numpy as np

    rng = np.random.default_rng(42)
    tickers = [f"T{i:03d}" for i in range(n_instruments)]
    sectors = ["Technology", "Healthcare", "Financials", "Energy", "Consumer"]

    return pd.DataFrame(
        {
            "instrument_id": list(range(1, n_instruments + 1)),
            "ticker": tickers,
            "company_name": [f"Company {t}" for t in tickers],
            "sector": [sectors[i % len(sectors)] for i in range(n_instruments)],
            "mom_1d":         rng.uniform(-0.05, 0.05, n_instruments),
            "mom_5d":         rng.uniform(-0.15, 0.15, n_instruments),
            "mom_21d":        rng.uniform(-0.20, 0.20, n_instruments),
            "mom_63d":        rng.uniform(-0.30, 0.30, n_instruments),
            "vol_20d_ann252": rng.uniform(0.10, 1.20, n_instruments),
            "mdd_252d":       rng.uniform(-0.60, 0.00, n_instruments),
            "dv_20d_log":     rng.uniform(10.0, 22.0, n_instruments),
            "vol_ratio_20d":  rng.uniform(0.3, 8.0, n_instruments),
            "decline_streak": rng.integers(0, 12, n_instruments).astype(float),
            "adj_close":      rng.uniform(10, 500, n_instruments),
            "adj_volume":     rng.uniform(1e5, 1e7, n_instruments),
            "intraday_range_pct": rng.uniform(0.005, 0.05, n_instruments),
        }
    )


def _make_price_df(n: int = 30) -> pd.DataFrame:
    import numpy as np
    rng = np.random.default_rng(7)
    return pd.DataFrame(
        {
            "instrument_id": list(range(1, n + 1)),
            "adj_close": rng.uniform(10, 500, n),
            "adj_volume": rng.uniform(1e5, 1e7, n),
            "high_price": rng.uniform(100, 510, n),
            "low_price": rng.uniform(90, 495, n),
            "open_price": rng.uniform(95, 505, n),
            "intraday_range_pct": rng.uniform(0.005, 0.05, n),
        }
    )


# ---------------------------------------------------------------------------
# Tests for generate_briefing
# ---------------------------------------------------------------------------

def test_generate_briefing_returns_expected_sections(monkeypatch, mock_conn):
    """All six section names must be present when data is available."""
    wide = _make_wide(40)
    prices = _make_price_df(40)

    monkeypatch.setattr("reports.daily_briefing._get_latest_factor_date", lambda conn: "2025-01-15")
    monkeypatch.setattr("reports.daily_briefing._load_factor_snapshot", lambda conn, date: wide)
    monkeypatch.setattr("reports.daily_briefing._load_price_snapshot", lambda conn, date: prices)

    result = generate_briefing(conn=mock_conn, date="2025-01-15", top_n=10)

    assert result["date"] == "2025-01-15"
    section_names = {s["name"] for s in result["sections"]}
    expected = {
        "momentum_leaders",
        "momentum_laggards",
        "volume_spikes",
        "decline_streaks",
        "volatility_alerts",
        "sector_summary",
    }
    assert expected.issubset(section_names), f"missing: {expected - section_names}"


def test_generate_briefing_top_n_respected(monkeypatch, mock_conn):
    """Each screener section must contain at most top_n rows."""
    wide = _make_wide(50)
    prices = _make_price_df(50)

    monkeypatch.setattr("reports.daily_briefing._get_latest_factor_date", lambda conn: "2025-02-01")
    monkeypatch.setattr("reports.daily_briefing._load_factor_snapshot", lambda conn, date: wide)
    monkeypatch.setattr("reports.daily_briefing._load_price_snapshot", lambda conn, date: prices)

    top_n = 5
    result = generate_briefing(conn=mock_conn, date="2025-02-01", top_n=top_n)

    for sec in result["sections"]:
        if sec["name"] == "sector_summary":
            continue  # sector count not bounded by top_n
        assert len(sec["rows"]) <= top_n, (
            f"section '{sec['name']}' has {len(sec['rows'])} rows, expected <= {top_n}"
        )


def test_generate_briefing_no_factor_data(monkeypatch, mock_conn):
    """Empty factor snapshot must return an empty sections list gracefully."""
    monkeypatch.setattr("reports.daily_briefing._get_latest_factor_date", lambda conn: "2025-03-01")
    monkeypatch.setattr(
        "reports.daily_briefing._load_factor_snapshot",
        lambda conn, date: pd.DataFrame(),
    )
    monkeypatch.setattr(
        "reports.daily_briefing._load_price_snapshot",
        lambda conn, date: pd.DataFrame(),
    )

    result = generate_briefing(conn=mock_conn, date="2025-03-01", top_n=20)

    assert result["date"] == "2025-03-01"
    assert result["sections"] == []


def test_generate_briefing_no_date_in_db(monkeypatch, mock_conn):
    """If there's no factor date at all, the function returns gracefully."""
    monkeypatch.setattr("reports.daily_briefing._get_latest_factor_date", lambda conn: None)

    result = generate_briefing(conn=mock_conn, top_n=20)

    assert result["date"] is None
    assert result["sections"] == []


def test_volume_spikes_section_filtered(monkeypatch, mock_conn):
    """Only instruments with vol_ratio >= threshold appear in volume_spikes."""
    import numpy as np
    import reports.daily_briefing as briefing_mod

    threshold = briefing_mod._VOL_SPIKE_MIN_RATIO  # default 2.0

    wide = _make_wide(40)
    # Force all vol_ratio values below threshold
    wide["vol_ratio_20d"] = 0.5

    prices = _make_price_df(40)

    monkeypatch.setattr("reports.daily_briefing._get_latest_factor_date", lambda conn: "2025-04-01")
    monkeypatch.setattr("reports.daily_briefing._load_factor_snapshot", lambda conn, date: wide)
    monkeypatch.setattr("reports.daily_briefing._load_price_snapshot", lambda conn, date: prices)

    result = generate_briefing(conn=mock_conn, date="2025-04-01", top_n=20)

    spikes_sec = next((s for s in result["sections"] if s["name"] == "volume_spikes"), None)
    assert spikes_sec is not None
    assert spikes_sec["rows"] == [], "No spikes expected when all ratios < threshold"


def test_decline_streaks_section_filtered(monkeypatch, mock_conn):
    """Only instruments with streak >= threshold appear in decline_streaks."""
    import reports.daily_briefing as briefing_mod

    threshold = briefing_mod._DECLINE_STREAK_MIN_DAYS  # default 3

    wide = _make_wide(40)
    wide["decline_streak"] = 1.0  # everyone has streak=1, below threshold

    prices = _make_price_df(40)

    monkeypatch.setattr("reports.daily_briefing._get_latest_factor_date", lambda conn: "2025-05-01")
    monkeypatch.setattr("reports.daily_briefing._load_factor_snapshot", lambda conn, date: wide)
    monkeypatch.setattr("reports.daily_briefing._load_price_snapshot", lambda conn, date: prices)

    result = generate_briefing(conn=mock_conn, date="2025-05-01", top_n=20)

    streak_sec = next((s for s in result["sections"] if s["name"] == "decline_streaks"), None)
    assert streak_sec is not None
    assert streak_sec["rows"] == []


# ---------------------------------------------------------------------------
# Tests for format_briefing
# ---------------------------------------------------------------------------

def test_format_briefing_returns_string(monkeypatch, mock_conn):
    """format_briefing must return a non-empty string."""
    wide = _make_wide(30)
    prices = _make_price_df(30)

    monkeypatch.setattr("reports.daily_briefing._get_latest_factor_date", lambda conn: "2025-06-01")
    monkeypatch.setattr("reports.daily_briefing._load_factor_snapshot", lambda conn, date: wide)
    monkeypatch.setattr("reports.daily_briefing._load_price_snapshot", lambda conn, date: prices)

    data = generate_briefing(conn=mock_conn, date="2025-06-01", top_n=10)
    text = format_briefing(data)

    assert isinstance(text, str)
    assert len(text) > 100


def test_format_briefing_contains_section_labels(monkeypatch, mock_conn):
    """The formatted text must include keywords from each section."""
    wide = _make_wide(30)
    prices = _make_price_df(30)

    monkeypatch.setattr("reports.daily_briefing._get_latest_factor_date", lambda conn: "2025-07-01")
    monkeypatch.setattr("reports.daily_briefing._load_factor_snapshot", lambda conn, date: wide)
    monkeypatch.setattr("reports.daily_briefing._load_price_snapshot", lambda conn, date: prices)

    data = generate_briefing(conn=mock_conn, date="2025-07-01", top_n=10)
    text = format_briefing(data)

    assert "MOMENTUM" in text
    assert "VOLUME" in text
    assert "SECTOR" in text
    assert "2025-07-01" in text


def test_format_briefing_empty_data():
    """format_briefing on an empty result must not crash."""
    empty = {"date": "2025-08-01", "sections": [], "raw_df": pd.DataFrame()}
    text = format_briefing(empty)
    assert isinstance(text, str)
    assert "2025-08-01" in text


def test_sector_summary_present(monkeypatch, mock_conn):
    """Sector summary rows must reflect distinct sectors from the data."""
    wide = _make_wide(25)
    prices = _make_price_df(25)

    monkeypatch.setattr("reports.daily_briefing._get_latest_factor_date", lambda conn: "2025-09-01")
    monkeypatch.setattr("reports.daily_briefing._load_factor_snapshot", lambda conn, date: wide)
    monkeypatch.setattr("reports.daily_briefing._load_price_snapshot", lambda conn, date: prices)

    result = generate_briefing(conn=mock_conn, date="2025-09-01", top_n=20)

    sector_sec = next((s for s in result["sections"] if s["name"] == "sector_summary"), None)
    assert sector_sec is not None
    assert len(sector_sec["rows"]) > 0

    sector_names = {r["sector"] for r in sector_sec["rows"]}
    assert "Technology" in sector_names
