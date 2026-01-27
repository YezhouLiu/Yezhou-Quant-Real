from __future__ import annotations

from typing import List, Dict, Optional, Any

import pandas as pd
from psycopg.types.json import Jsonb
from utils.logger import get_logger

log = get_logger("rw_factor_values")


# -----------------------------------------------------------------------------
# Insert / Upsert
# -----------------------------------------------------------------------------
def insert_factor_value(
    conn,
    *,
    instrument_id: int,
    date: str,
    factor_name: str,
    factor_value: float,
    factor_version: str = "v1",
    factor_args: Optional[Dict] = None,
    config: Optional[Dict] = None,
    data_source: str = "internal",
):
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO factor_values (
            instrument_id, date, factor_name,
            factor_value, factor_version,
            factor_args, config, data_source
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (instrument_id, date, factor_name, factor_version)
        DO UPDATE SET
            factor_value = EXCLUDED.factor_value,
            factor_args  = EXCLUDED.factor_args,
            config       = EXCLUDED.config,
            data_source  = EXCLUDED.data_source,
            ingested_at  = now()
        """,
        (
            instrument_id,
            date,
            factor_name,
            factor_value,
            factor_version,
            Jsonb(factor_args or {}),
            Jsonb(config or {}),
            data_source,
        ),
    )


def batch_insert_factor_values(conn, rows: List[Dict[str, Any]]):
    if not rows:
        return

    normalized = []
    for r in rows:
        normalized.append(
            {
                "instrument_id": r["instrument_id"],
                "date": r["date"],
                "factor_name": r["factor_name"],
                "factor_value": r["factor_value"],
                "factor_version": r.get("factor_version", "v1"),
                "factor_args": Jsonb(r.get("factor_args", {})),
                "config": Jsonb(r.get("config", {})),
                "data_source": r.get("data_source", "internal"),
            }
        )

    cursor = conn.cursor()
    cursor.executemany(
        """
        INSERT INTO factor_values (
            instrument_id, date, factor_name,
            factor_value, factor_version,
            factor_args, config, data_source
        )
        VALUES (
            %(instrument_id)s,
            %(date)s,
            %(factor_name)s,
            %(factor_value)s,
            %(factor_version)s,
            %(factor_args)s,
            %(config)s,
            %(data_source)s
        )
        ON CONFLICT (instrument_id, date, factor_name, factor_version)
        DO UPDATE SET
            factor_value = EXCLUDED.factor_value,
            factor_args  = EXCLUDED.factor_args,
            config       = EXCLUDED.config,
            data_source  = EXCLUDED.data_source,
            ingested_at  = now()
        """,
        normalized,
    )

    log.info(f"[rw_factor_values] wrote {len(normalized)} rows")


# -----------------------------------------------------------------------------
# Query
# -----------------------------------------------------------------------------
def get_factor_values(
    conn,
    *,
    factor_name: Optional[str] = None,
    factor_version: Optional[str] = None,
    instrument_id: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    query = "SELECT * FROM factor_values WHERE 1=1"
    params: List[Any] = []

    if factor_name is not None:
        query += " AND factor_name = %s"
        params.append(factor_name)

    if factor_version is not None:
        query += " AND factor_version = %s"
        params.append(factor_version)

    if instrument_id is not None:
        query += " AND instrument_id = %s"
        params.append(instrument_id)

    if start_date is not None:
        query += " AND date >= %s"
        params.append(start_date)

    if end_date is not None:
        query += " AND date <= %s"
        params.append(end_date)

    query += " ORDER BY date"

    cursor = conn.cursor()
    cursor.execute(query, params)

    cols = [d[0] for d in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=cols)


def get_latest_factor_value(
    conn,
    *,
    instrument_id: int,
    factor_name: str,
    factor_version: str = "v1",
) -> Optional[float]:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT factor_value
        FROM factor_values
        WHERE instrument_id = %s
          AND factor_name = %s
          AND factor_version = %s
        ORDER BY date DESC
        LIMIT 1
        """,
        (instrument_id, factor_name, factor_version),
    )

    row = cursor.fetchone()
    return row[0] if row else None


def get_factor_snapshot(
    conn,
    *,
    factor_name: str,
    date: str,
    factor_version: str = "v1",
) -> pd.DataFrame:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT instrument_id, factor_value
        FROM factor_values
        WHERE factor_name = %s
          AND date = %s
          AND factor_version = %s
        """,
        (factor_name, date, factor_version),
    )

    cols = [d[0] for d in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=cols)


# -----------------------------------------------------------------------------
# Delete
# -----------------------------------------------------------------------------
def delete_factor_values(
    conn,
    *,
    factor_name: str,
    factor_version: Optional[str] = None,
):
    query = "DELETE FROM factor_values WHERE factor_name = %s"
    params = [factor_name]

    if factor_version is not None:
        query += " AND factor_version = %s"
        params.append(factor_version)

    cursor = conn.cursor()
    cursor.execute(query, params)

    log.warning(
        f"[rw_factor_values] deleted factor_name={factor_name}, version={factor_version}"
    )
