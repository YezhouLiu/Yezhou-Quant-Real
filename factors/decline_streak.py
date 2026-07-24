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

log = get_logger("factor_decline_streak")

FACTOR_NAME = "decline_streak"

# Maximum consecutive-decline look-back: prevents runaway streaks from bad data
_MAX_STREAK_CAP = 252


def calc_single_instrument_decline_streak(
    conn: Connection,
    instrument_id: int,
    start_date,
    end_date,
    *,
    factor_version: str = "v1",
) -> int:
    """
    Counts consecutive trading days where adj_close is strictly lower than the
    previous trading day, as of each date in [start_date, end_date].

    A value of 5 means the stock has closed lower for 5 consecutive days.
    A value of 0 means today's close was flat or higher than yesterday's.
    """
    start_date = to_date(start_date)
    end_date = to_date(end_date)

    if start_date > end_date:
        return 0

    # Load enough history to capture streaks that started before start_date
    buffer_days = _MAX_STREAK_CAP * 2
    load_start = (pd.Timestamp(start_date) - pd.Timedelta(days=buffer_days)).date()

    try:
        df = get_prices(
            conn,
            instrument_id=instrument_id,
            start_date=load_start.isoformat(),
            end_date=end_date.isoformat(),
        )
    except Exception as e:
        log.warning(f"[decline_streak] instrument={instrument_id} get_prices failed: {e}")
        return 0

    if df.empty:
        return 0

    required_cols = {"instrument_id", "date", "adj_close"}
    missing = required_cols - set(df.columns)
    if missing:
        log.warning(
            f"[decline_streak] instrument={instrument_id} missing columns: {sorted(missing)}"
        )
        return 0

    df = df[["instrument_id", "date", "adj_close"]].copy()

    try:
        df["date"] = pd.to_datetime(df["date"])
        df["adj_close"] = pd.to_numeric(df["adj_close"], errors="coerce")
    except Exception as e:
        log.warning(f"[decline_streak] instrument={instrument_id} parse failed: {e}")
        return 0

    df = df.sort_values("date").reset_index(drop=True)

    # Boolean: True when today's close is strictly below yesterday's
    df["is_down"] = df["adj_close"] < df["adj_close"].shift(1)

    # Vectorized consecutive-run counter:
    # Each time is_down flips to False we start a new group; cumsum within the
    # group of True values gives the streak length at every row.
    flip_groups = (~df["is_down"]).cumsum()
    df["decline_streak"] = df["is_down"].groupby(flip_groups).cumsum().astype(int)

    target = df[
        (df["date"] >= pd.to_datetime(start_date))
        & (df["date"] <= pd.to_datetime(end_date))
    ]

    # is_down / decline_streak are already computed on the full df,
    # so shift-alignment is correct. Only exclude rows where the close
    # itself is NaN (can't confirm whether the day was actually a decline).
    valid = target["adj_close"].notna()
    valid_rows = target.loc[valid]

    if valid_rows.empty:
        return 0

    batch_rows = [
        {
            "instrument_id": instrument_id,
            "date": r.date.date().isoformat(),
            "factor_name": FACTOR_NAME,
            "factor_value": float(r.decline_streak),
            "factor_version": factor_version,
            "factor_args": {},
            "config": {},
            "data_source": "internal",
        }
        for r in valid_rows.itertuples(index=False)
    ]

    try:
        batch_insert_factor_values(conn, batch_rows)
        conn.commit()
    except Exception as e:
        log.warning(f"[decline_streak] instrument={instrument_id} db write failed: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0

    return len(batch_rows)
