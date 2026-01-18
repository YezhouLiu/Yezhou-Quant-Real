from typing import List, Dict, Optional, Any
import pandas as pd
from psycopg.types.json import Jsonb
from utils.logger import get_logger

log = get_logger("rw_corporate_actions")


def insert_corporate_action(
    conn,
    instrument_id: int,
    action_date: str,
    action_type: str,
    action_value: Optional[float] = None,
    currency: str = "USD",
    data_source: str = "tiingo",
    raw_payload: Optional[Dict[str, Any]] = None,
):
    """插入单条公司行为数据（UPSERT）"""
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO corporate_actions (
            instrument_id, action_date, action_type,
            action_value, currency, data_source, raw_payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (instrument_id, action_date, action_type) DO UPDATE SET
            action_value = EXCLUDED.action_value,
            currency = EXCLUDED.currency,
            data_source = EXCLUDED.data_source,
            raw_payload = EXCLUDED.raw_payload,
            ingested_at = now()
        """,
        (
            instrument_id,
            action_date,
            action_type,
            action_value,
            currency,
            data_source,
            Jsonb(raw_payload) if raw_payload is not None else None,
        ),
    )


def batch_insert_corporate_actions(conn, actions: List[Dict[str, Any]]):
    """批量插入公司行为数据（逐条 execute，保持简单可控）"""
    cursor = conn.cursor()

    for a in actions:
        cursor.execute(
            """
            INSERT INTO corporate_actions (
                instrument_id, action_date, action_type,
                action_value, currency, data_source, raw_payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (instrument_id, action_date, action_type) DO UPDATE SET
                action_value = EXCLUDED.action_value,
                currency = EXCLUDED.currency,
                data_source = EXCLUDED.data_source,
                raw_payload = EXCLUDED.raw_payload,
                ingested_at = now()
            """,
            (
                a["instrument_id"],
                a["action_date"],
                a["action_type"],
                a.get("action_value"),
                a.get("currency", "USD"),
                a.get("data_source", "tiingo"),
                Jsonb(a["raw_payload"]) if a.get("raw_payload") is not None else None,
            ),
        )

    log.info(f"[✔] 批量插入 {len(actions)} 条公司行为数据")


def get_corporate_actions(
    conn,
    instrument_id: int,
    action_type: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """获取公司行为数据"""
    query = "SELECT * FROM corporate_actions WHERE instrument_id = %s"
    params: List[Any] = [instrument_id]

    if action_type:
        query += " AND action_type = %s"
        params.append(action_type)

    if start_date:
        query += " AND action_date >= %s"
        params.append(start_date)

    if end_date:
        query += " AND action_date <= %s"
        params.append(end_date)

    query += " ORDER BY action_date DESC"

    cursor = conn.cursor()
    cursor.execute(query, params)

    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def get_latest_corporate_action_date(
    conn,
    instrument_id: int,
    action_type: Optional[str] = None,
) -> Optional[str]:
    """获取某资产公司行为的最新日期（可按 action_type 过滤）"""
    cursor = conn.cursor()

    if action_type:
        cursor.execute(
            """
            SELECT action_date FROM corporate_actions
            WHERE instrument_id = %s AND action_type = %s
            ORDER BY action_date DESC
            LIMIT 1
            """,
            (instrument_id, action_type),
        )
    else:
        cursor.execute(
            """
            SELECT action_date FROM corporate_actions
            WHERE instrument_id = %s
            ORDER BY action_date DESC
            LIMIT 1
            """,
            (instrument_id,),
        )

    row = cursor.fetchone()
    if not row:
        return None

    # psycopg 通常会返回 date；这里统一转 str，方便上游用于 state / log
    d = row[0]
    return d.isoformat() if hasattr(d, "isoformat") else str(d)


def delete_corporate_actions(
    conn,
    instrument_id: int,
    action_type: Optional[str] = None,
):
    """删除公司行为数据（可按 action_type 过滤）"""
    query = "DELETE FROM corporate_actions WHERE instrument_id = %s"
    params: List[Any] = [instrument_id]

    if action_type:
        query += " AND action_type = %s"
        params.append(action_type)

    cursor = conn.cursor()
    cursor.execute(query, params)
    log.warning(f"[⚠] 删除公司行为数据: instrument_id={instrument_id}, action_type={action_type}")