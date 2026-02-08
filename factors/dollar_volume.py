from __future__ import annotations

import math
import pandas as pd
from psycopg import Connection

from database.readwrite.rw_market_prices import get_prices
from database.readwrite.rw_factor_values import batch_insert_factor_values
from utils.logger import get_logger
from utils.time import to_date

log = get_logger("factor_dollar_volume")


def _infer_factor_name(window: int) -> str:
    return f"dv_{window}d_log"


def calc_single_instrument_dollar_volume(
    conn: Connection,
    instrument_id: int,
    start_date,
    end_date,
    *,
    window: int = 20,
    factor_version: str = "v1",
) -> int:
    start_date = to_date(start_date)
    end_date = to_date(end_date)

    if start_date > end_date:
        return 0

    factor_name = _infer_factor_name(window)
    factor_args = {
        "window": window,
        "transform": "log",
        "field": "adj_close*adj_volume",
    }

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
        log.warning(
            f"[dollar_volume] instrument={instrument_id} get_prices failed: {e}"
        )
        return 0

    if df.empty:
        return 0

    required_cols = {"instrument_id", "date", "adj_close", "adj_volume"}
    missing = required_cols - set(df.columns)
    if missing:
        log.warning(
            f"[dollar_volume] instrument={instrument_id} missing columns: {sorted(missing)}"
        )
        return 0

    df = df[["instrument_id", "date", "adj_close", "adj_volume"]].rename(
        columns={"adj_close": "price", "adj_volume": "volume"}
    )

    try:
        df["date"] = pd.to_datetime(df["date"])
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    except Exception as e:
        log.warning(f"[dollar_volume] instrument={instrument_id} parse failed: {e}")
        return 0

    df = df.sort_values("date")

    # dollar volume: price * volume
    df["_dollar_vol"] = df["price"] * df["volume"]

    # validity: price > 0, volume >= 0, dollar_vol > 0
    valid_dv = (df["price"] > 0) & (df["volume"] >= 0) & (df["_dollar_vol"] > 0)

    # rolling mean then log
    dv_mean = df["_dollar_vol"].where(valid_dv).rolling(window).mean()
    df["factor_value"] = dv_mean.map(
        lambda x: math.log(x) if pd.notna(x) and x > 0 else pd.NA
    )

    target = df[
        (df["date"] >= pd.to_datetime(start_date))
        & (df["date"] <= pd.to_datetime(end_date))
    ]

    valid = target["factor_value"].notna()
    valid_rows = target.loc[valid]

    if valid_rows.empty:
        if len(target) >= 200:
            null_price = int(target["price"].isna().sum())
            null_vol = int(target["volume"].isna().sum())
            log.debug(
                f"[dollar_volume] instrument={instrument_id} produced 0 rows "
                f"(target={len(target)}, null_price={null_price}, null_volume={null_vol}, "
                f"window={window}, range={start_date}->{end_date})"
            )
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
        log.warning(f"[dollar_volume] instrument={instrument_id} db write failed: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0

    return len(batch_rows)
