from typing import List, Dict, Optional
import pandas as pd
from utils.logger import get_logger

log = get_logger("rw_fundamental_data")


def insert_fundamental(conn, instrument_id: int, metric_name: str, value: float,
                      as_of_date: str, period_type: str = 'annual', source: str = None):
    """插入基本面数据"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO fundamental_data (instrument_id, metric_name, value, as_of_date, period_type, source)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (instrument_id, metric_name, as_of_date, period_type) DO UPDATE SET
            value = EXCLUDED.value,
            source = EXCLUDED.source,
            ingested_at = now()
    """, (instrument_id, metric_name, value, as_of_date, period_type, source))


def batch_insert_fundamentals(conn, fundamentals: List[Dict]):
    """批量插入基本面数据"""
    cursor = conn.cursor()
    
    for fund in fundamentals:
        cursor.execute("""
            INSERT INTO fundamental_data (instrument_id, metric_name, value, as_of_date, period_type, source)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (instrument_id, metric_name, as_of_date, period_type) DO UPDATE SET
                value = EXCLUDED.value,
                ingested_at = now()
        """, (
            fund['instrument_id'], fund['metric_name'], fund['value'],
            fund['as_of_date'], fund.get('period_type', 'annual'), fund.get('source')
        ))
    
    log.info(f"[✔] 批量插入 {len(fundamentals)} 条基本面数据")


def get_fundamentals(conn, instrument_id: int, metric_name: str = None,
                    start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """获取基本面数据"""
    query = "SELECT * FROM fundamental_data WHERE instrument_id = %s"
    params = [instrument_id]
    
    if metric_name:
        query += " AND metric_name = %s"
        params.append(metric_name)
    
    if start_date:
        query += " AND as_of_date >= %s"
        params.append(start_date)
    
    if end_date:
        query += " AND as_of_date <= %s"
        params.append(end_date)
    
    query += " ORDER BY as_of_date DESC"
    
    cursor = conn.cursor()
    cursor.execute(query, params)
    
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def get_latest_fundamental(conn, instrument_id: int, metric_name: str) -> Optional[float]:
    """获取最新的基本面指标"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT value FROM fundamental_data
        WHERE instrument_id = %s AND metric_name = %s
        ORDER BY as_of_date DESC
        LIMIT 1
    """, (instrument_id, metric_name))
    
    result = cursor.fetchone()
    return result[0] if result else None


def delete_fundamentals(conn, instrument_id: int, metric_name: str = None):
    """删除基本面数据"""
    query = "DELETE FROM fundamental_data WHERE instrument_id = %s"
    params = [instrument_id]
    
    if metric_name:
        query += " AND metric_name = %s"
        params.append(metric_name)
    
    cursor = conn.cursor()
    cursor.execute(query, params)
    log.warning(f"[⚠] 删除基本面数据: instrument_id={instrument_id}")
