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
from __future__ import annotations

import pandas as pd
from psycopg import Connection

from database.readwrite.rw_market_prices import get_prices
from database.readwrite.rw_factor_values import batch_insert_factor_values
from utils.logger import get_logger
from utils.time import to_date

log = get_logger("factor_volume_ratio")


def _infer_factor_name(window: int) -> str:
    return f"vol_ratio_{window}d"


def calc_single_instrument_volume_ratio(
    conn: Connection,
    instrument_id: int,
    start_date,
    end_date,
    *,
    window: int = 20,
    factor_version: str = "v1",
) -> int:
    """
    Volume ratio: today's adj_volume / rolling mean of the previous `window` days.

    A value of 2.0 means today's volume is 2x the recent average — a potential
    event-driven signal worth investigating.
    """
    start_date = to_date(start_date)
    end_date = to_date(end_date)

    if start_date > end_date:
        return 0

    factor_name = _infer_factor_name(window)
    factor_args = {"window": window, "field": "adj_volume"}

    # Extra buffer so rolling mean of the day before start_date is already warm
    buffer_days = (window + 10) * 2
    load_start = (pd.Timestamp(start_date) - pd.Timedelta(days=buffer_days)).date()

    try:
        df = get_prices(
            conn,
            instrument_id=instrument_id,
            start_date=load_start.isoformat(),
            end_date=end_date.isoformat(),
        )
    except Exception as e:
        log.warning(f"[volume_ratio] instrument={instrument_id} get_prices failed: {e}")
        return 0

    if df.empty:
        return 0

    required_cols = {"instrument_id", "date", "adj_volume"}
    missing = required_cols - set(df.columns)
    if missing:
        log.warning(
            f"[volume_ratio] instrument={instrument_id} missing columns: {sorted(missing)}"
        )
        return 0

    df = df[["instrument_id", "date", "adj_volume"]].copy()

    try:
        df["date"] = pd.to_datetime(df["date"])
        df["adj_volume"] = pd.to_numeric(df["adj_volume"], errors="coerce")
    except Exception as e:
        log.warning(f"[volume_ratio] instrument={instrument_id} parse failed: {e}")
        return 0

    df = df.sort_values("date").reset_index(drop=True)

    # Mask non-positive volume before computing the rolling mean
    valid_vol = df["adj_volume"].where(df["adj_volume"] > 0)

    # shift(1) so the average excludes today — we're comparing today vs. the past
    avg_vol = valid_vol.rolling(window).mean().shift(1)

    df["factor_value"] = df["adj_volume"] / avg_vol

    target = df[
        (df["date"] >= pd.to_datetime(start_date))
        & (df["date"] <= pd.to_datetime(end_date))
    ]

    valid = (
        target["factor_value"].notna()
        & target["factor_value"].gt(0)
        & target["adj_volume"].gt(0)
    )
    valid_rows = target.loc[valid]

    if valid_rows.empty:
        return 0

    batch_rows = [
        {
            "instrument_id": instrument_id,
            "date": r.date.date().isoformat(),
            "factor_name": factor_name,
            "factor_value": float(r.factor_value),
            "factor_version": factor_version,
            "factor_args": factor_args,
            "config": {},
            "data_source": "internal",
        }
        for r in valid_rows.itertuples(index=False)
    ]

    try:
        batch_insert_factor_values(conn, batch_rows)
        conn.commit()
    except Exception as e:
        log.warning(f"[volume_ratio] instrument={instrument_id} db write failed: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0

    return len(batch_rows)
