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
    factor_names: Optional[List[str]] = None,
    factor_version: Optional[str] = None,
    instrument_id: Optional[int] = None,
    instrument_ids: Optional[List[int]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    date: Optional[str] = None,
) -> pd.DataFrame:
    """
    查询因子值
    
    Parameters
    ----------
    factor_name : 单个因子名（与 factor_names 二选一）
    factor_names : 因子名列表（与 factor_name 二选一）
    factor_version : 因子版本
    instrument_id : 单个标的 ID（与 instrument_ids 二选一）
    instrument_ids : 标的 ID 列表（与 instrument_id 二选一）
    start_date : 起始日期（>=）
    end_date : 结束日期（<=）
    date : 精确日期（与 start_date/end_date 互斥）
    """
    query = "SELECT * FROM factor_values WHERE 1=1"
    params: List[Any] = []

    # 因子名：单个或列表
    if factor_name is not None and factor_names is not None:
        raise ValueError("factor_name and factor_names are mutually exclusive")
    
    if factor_name is not None:
        query += " AND factor_name = %s"
        params.append(factor_name)
    elif factor_names is not None:
        if len(factor_names) == 0:
            raise ValueError("factor_names cannot be empty")
        query += " AND factor_name = ANY(%s)"
        params.append(factor_names)

    if factor_version is not None:
        query += " AND factor_version = %s"
        params.append(factor_version)

    # 标的 ID：单个或列表
    if instrument_id is not None and instrument_ids is not None:
        raise ValueError("instrument_id and instrument_ids are mutually exclusive")
    
    if instrument_id is not None:
        query += " AND instrument_id = %s"
        params.append(instrument_id)
    elif instrument_ids is not None:
        if len(instrument_ids) == 0:
            raise ValueError("instrument_ids cannot be empty")
        query += " AND instrument_id = ANY(%s)"
        params.append(instrument_ids)

    # 日期：精确日期或范围
    if date is not None and (start_date is not None or end_date is not None):
        raise ValueError("date is mutually exclusive with start_date/end_date")
    
    if date is not None:
        query += " AND date = %s"
        params.append(date)
    else:
        if start_date is not None:
            query += " AND date >= %s"
            params.append(start_date)
        if end_date is not None:
            query += " AND date <= %s"
            params.append(end_date)

    query += " ORDER BY date, instrument_id"

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
