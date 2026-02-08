from __future__ import annotations

import numpy as np
import pandas as pd
from psycopg import Connection

from database.readwrite.rw_market_prices import get_prices
from database.readwrite.rw_factor_values import batch_insert_factor_values
from utils.logger import get_logger
from utils.time import to_date

log = get_logger("factor_volatility_of_volatility")


def _infer_factor_name(vol_window: int, volvol_window: int) -> str:
    return f"volvol_{volvol_window}d_from_vol{vol_window}d"


def calc_single_instrument_volatility_of_volatility(
    conn: Connection,
    instrument_id: int,
    start_date,
    end_date,
    *,
    vol_window: int = 20,
    volvol_window: int = 60,
    annualize: int = 252,
    factor_version: str = "v1",
) -> int:
    start_date = to_date(start_date)
    end_date = to_date(end_date)

    if start_date > end_date:
        return 0

    factor_name = _infer_factor_name(vol_window, volvol_window)
    factor_args = {
        "vol_window": vol_window,
        "volvol_window": volvol_window,
        "annualize": annualize,
    }

    buffer_days = (vol_window + volvol_window + 10) * 2
    load_start = (pd.Timestamp(start_date) - pd.Timedelta(days=buffer_days)).date()

    try:
        df = get_prices(
            conn,
            instrument_id=instrument_id,
            start_date=load_start.isoformat(),
            end_date=end_date.isoformat(),
        )
    except Exception as e:
        log.warning(f"[volvol] instrument={instrument_id} get_prices failed: {e}")
        return 0

    if df.empty:
        return 0

    required_cols = {"instrument_id", "date", "adj_close"}
    missing = required_cols - set(df.columns)
    if missing:
        log.warning(
            f"[volvol] instrument={instrument_id} missing columns: {sorted(missing)}"
        )
        return 0

    df = df[["instrument_id", "date", "adj_close"]].rename(
        columns={"adj_close": "price"}
    )

    try:
        df["date"] = pd.to_datetime(df["date"])
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
    except Exception as e:
        log.warning(f"[volvol] instrument={instrument_id} parse failed: {e}")
        return 0

    df = df.sort_values("date")

    # log returns
    logp = np.log(df["price"].astype("float64"))
    logp = pd.Series(logp).replace([np.inf, -np.inf], np.nan)
    ret = logp.diff()

    # first-layer volatility
    vol = ret.rolling(vol_window).std() * np.sqrt(float(annualize))

    # second-layer volatility (vol of vol)
    df["factor_value"] = vol.rolling(volvol_window).std()

    target = df[
        (df["date"] >= pd.to_datetime(start_date))
        & (df["date"] <= pd.to_datetime(end_date))
    ]

    valid = target["factor_value"].notna()
    valid_rows = target.loc[valid]

    if valid_rows.empty:
        if len(target) >= 200:
            null_price = int(target["price"].isna().sum())
            log.debug(
                f"[volvol] instrument={instrument_id} produced 0 rows "
                f"(target={len(target)}, null_price={null_price}, "
                f"vol_window={vol_window}, volvol_window={volvol_window}, "
                f"range={start_date}->{end_date})"
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
        log.warning(f"[volvol] instrument={instrument_id} db write failed: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0

    return len(batch_rows)
