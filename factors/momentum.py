from __future__ import annotations

import pandas as pd
from psycopg import Connection
from database.readwrite.rw_market_prices import get_prices
from database.readwrite.rw_factor_values import batch_insert_factor_values
from utils.logger import get_logger
from utils.time import to_date

log = get_logger("factor_momentum")


def _infer_factor_name(lookback: int, skip: int) -> str:
    return f"mom_{lookback}d_skip{skip}" if skip > 0 else f"mom_{lookback}d"


def calc_single_instrument_momentum(
    conn: Connection,
    instrument_id: int,
    start_date,
    end_date,
    lookback: int,
    skip: int,
    factor_version: str = "v1",
) -> int:
    start_date = to_date(start_date)
    end_date = to_date(end_date)

    if start_date > end_date:
        return 0

    factor_name = _infer_factor_name(lookback, skip)
    factor_args = {"lookback": lookback, "skip": skip}

    buffer_days = (lookback + skip + 10) * 2
    load_start = (pd.Timestamp(start_date) - pd.Timedelta(days=buffer_days)).date()

    try:
        df = get_prices(
            conn,
            instrument_id=instrument_id,
            start_date=load_start.isoformat(),
            end_date=end_date.isoformat(),
        )
    except Exception as e:
        log.warning(f"[momentum] instrument={instrument_id} get_prices failed: {e}")
        return 0

    if df.empty:
        return 0

    # 必需列缺失：这是数据/接口错误，值得打一条 log
    required_cols = {"instrument_id", "date", "adj_close"}
    missing = required_cols - set(df.columns)
    if missing:
        log.warning(
            f"[momentum] instrument={instrument_id} missing columns: {sorted(missing)}"
        )
        return 0

    df = df[["instrument_id", "date", "adj_close"]].rename(
        columns={"adj_close": "price"}
    )

    try:
        df["date"] = pd.to_datetime(df["date"])
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
    except Exception as e:
        log.warning(f"[momentum] instrument={instrument_id} parse failed: {e}")
        return 0

    df = df.sort_values("date")

    df["_price_t0"] = df["price"].shift(skip)
    df["_price_t1"] = df["price"].shift(skip + lookback)
    df["factor_value"] = df["_price_t0"] / df["_price_t1"] - 1.0

    target = df[
        (df["date"] >= pd.to_datetime(start_date))
        & (df["date"] <= pd.to_datetime(end_date))
    ]

    valid = (
        (target["_price_t0"] > 0)
        & (target["_price_t1"] > 0)
        & target["factor_value"].notna()
    )

    valid_rows = target.loc[valid]

    if valid_rows.empty:
        if len(target) >= 200:
            null_price = int(target["price"].isna().sum())
            log.debug(
                f"[momentum] instrument={instrument_id} produced 0 rows "
                f"(target={len(target)}, null_price={null_price}, "
                f"lookback={lookback}, skip={skip}, range={start_date}->{end_date})"
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
        log.warning(f"[momentum] instrument={instrument_id} db write failed: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0

    return len(batch_rows)
