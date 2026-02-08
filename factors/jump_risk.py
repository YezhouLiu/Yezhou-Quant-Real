from __future__ import annotations

import pandas as pd
from psycopg import Connection

from database.readwrite.rw_market_prices import get_prices
from database.readwrite.rw_factor_values import batch_insert_factor_values
from utils.logger import get_logger
from utils.time import to_date
from utils.config_values import (
    DEFAULT_JUMP_THRESHOLD,
    DEFAULT_JUMP_RATIO_LIMIT,
)

log = get_logger("factor_jump_risk")


def _infer_factor_names(window: int) -> tuple[str, str]:
    return f"jump_{window}d_max", f"jump_{window}d_cnt"


def calc_single_instrument_jump_risk(
    conn: Connection,
    instrument_id: int,
    start_date,
    end_date,
    *,
    window: int = 60,
    jump_threshold: float = DEFAULT_JUMP_THRESHOLD(),
    jump_ratio_limit: float = DEFAULT_JUMP_RATIO_LIMIT(),
    factor_version: str = "v1",
) -> int:
    start_date = to_date(start_date)
    end_date = to_date(end_date)

    if start_date > end_date:
        return 0

    factor_name_max, factor_name_cnt = _infer_factor_names(window)
    factor_args = {
        "window": window,
        "jump_threshold": jump_threshold,
        "jump_ratio_limit": jump_ratio_limit,
    }

    buffer_days = (window + 5) * 2
    load_start = (pd.Timestamp(start_date) - pd.Timedelta(days=buffer_days)).date()

    try:
        df = get_prices(
            conn,
            instrument_id=instrument_id,
            start_date=load_start.isoformat(),
            end_date=end_date.isoformat(),
        )
    except Exception as e:
        log.warning(f"[jump] instrument={instrument_id} get_prices failed: {e}")
        return 0

    if df.empty:
        return 0

    required_cols = {"instrument_id", "date", "adj_close"}
    missing = required_cols - set(df.columns)
    if missing:
        log.warning(
            f"[jump] instrument={instrument_id} missing columns: {sorted(missing)}"
        )
        return 0

    df = df[["instrument_id", "date", "adj_close"]].rename(
        columns={"adj_close": "price"}
    )

    try:
        df["date"] = pd.to_datetime(df["date"])
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
    except Exception as e:
        log.warning(f"[jump] instrument={instrument_id} parse failed: {e}")
        return 0

    df = df.sort_values("date")

    # daily gap
    prev_price = df["price"].shift(1)
    gap = (df["price"] / prev_price - 1.0).abs()

    is_jump = (
        (gap >= jump_threshold)
        & (gap <= jump_ratio_limit)
    )

    df["_jump"] = gap.where(is_jump)

    # 用 0 填充非 jump 日，再 rolling
    jump_val = gap.where(is_jump, 0.0)

    df["jump_max"] = jump_val.rolling(window).max()
    df["jump_cnt"] = is_jump.rolling(window).sum()

    target = df[
        (df["date"] >= pd.to_datetime(start_date))
        & (df["date"] <= pd.to_datetime(end_date))
    ]

    valid = target["jump_max"].notna()
    valid_rows = target.loc[valid]

    if valid_rows.empty:
        return 0

    batch_rows = []
    for r in valid_rows.itertuples(index=False):
        date_str = r.date.date().isoformat()

        batch_rows.append(
            {
                "instrument_id": instrument_id,
                "date": date_str,
                "factor_name": factor_name_max,
                "factor_value": float(r.jump_max),
                "factor_version": factor_version,
                "factor_args": factor_args,
                "config": {},
                "data_source": "internal",
            }
        )
        batch_rows.append(
            {
                "instrument_id": instrument_id,
                "date": date_str,
                "factor_name": factor_name_cnt,
                "factor_value": float(r.jump_cnt),
                "factor_version": factor_version,
                "factor_args": factor_args,
                "config": {},
                "data_source": "internal",
            }
        )

    try:
        batch_insert_factor_values(conn, batch_rows)
        conn.commit()
    except Exception as e:
        log.warning(f"[jump] instrument={instrument_id} db write failed: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0

    return len(batch_rows)
