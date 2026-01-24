from typing import List, Dict, Optional
import pandas as pd
from utils.logger import get_logger

log = get_logger("rw_fundamental_daily")


def insert_fundamental_daily(
    conn,
    instrument_id: int,
    metric_name: str,
    value: float,
    date: str,
    currency: str = "USD",
    source: str = None,
):
    """插入单条每日基本面/估值数据"""
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO fundamental_daily (
            instrument_id, date, metric_name,
            metric_value, currency, data_source
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (instrument_id, date, metric_name)
        DO UPDATE SET
            metric_value = EXCLUDED.metric_value,
            currency = EXCLUDED.currency,
            data_source = EXCLUDED.data_source,
            ingested_at = now()
        """,
        (instrument_id, date, metric_name, value, currency, source),
    )


def batch_insert_fundamental_daily(conn, records: List[Dict]):
    """批量插入每日基本面/估值数据"""
    cursor = conn.cursor()

    for rec in records:
        cursor.execute(
            """
            INSERT INTO fundamental_daily (
                instrument_id, date, metric_name,
                metric_value, currency, data_source
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (instrument_id, date, metric_name)
            DO UPDATE SET
                metric_value = EXCLUDED.metric_value,
                currency = EXCLUDED.currency,
                data_source = EXCLUDED.data_source,
                ingested_at = now()
            """,
            (
                rec["instrument_id"],
                rec["date"],
                rec["metric_name"],
                rec["value"],
                rec.get("currency", "USD"),
                rec.get("source"),
            ),
        )

    log.info(f"[✔] 批量插入 {len(records)} 条 fundamental_daily 数据")


def get_fundamental_daily(
    conn,
    instrument_id: int,
    metric_name: str = None,
    start_date: str = None,
    end_date: str = None,
) -> pd.DataFrame:
    """获取每日基本面/估值数据"""
    query = "SELECT * FROM fundamental_daily WHERE instrument_id = %s"
    params = [instrument_id]

    if metric_name:
        query += " AND metric_name = %s"
        params.append(metric_name)

    if start_date:
        query += " AND date >= %s"
        params.append(start_date)

    if end_date:
        query += " AND date <= %s"
        params.append(end_date)

    query += " ORDER BY date DESC"

    cursor = conn.cursor()
    cursor.execute(query, params)

    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def get_latest_fundamental_daily(
    conn,
    instrument_id: int,
    metric_name: str,
) -> Optional[float]:
    """获取某个每日指标的最新值"""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT metric_value
        FROM fundamental_daily
        WHERE instrument_id = %s AND metric_name = %s
        ORDER BY date DESC
        LIMIT 1
        """,
        (instrument_id, metric_name),
    )

    result = cursor.fetchone()
    return result[0] if result else None


def delete_fundamental_daily(
    conn,
    instrument_id: int,
    metric_name: str = None,
):
    """删除每日基本面/估值数据"""
    query = "DELETE FROM fundamental_daily WHERE instrument_id = %s"
    params = [instrument_id]

    if metric_name:
        query += " AND metric_name = %s"
        params.append(metric_name)

    cursor = conn.cursor()
    cursor.execute(query, params)

    log.warning(
        f"[⚠] 删除 fundamental_daily 数据: instrument_id={instrument_id}, metric={metric_name}"
    )
