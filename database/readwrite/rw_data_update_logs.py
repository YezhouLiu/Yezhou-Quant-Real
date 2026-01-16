from typing import Optional
import pandas as pd
from utils.logger import get_logger

log = get_logger("rw_data_update_logs")


def create_log(conn, dataset: str, source: str, start_date: str = None,
              end_date: str = None, instruments_count: int = None) -> int:
    """创建数据更新日志"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO data_update_logs (dataset, source, start_date, end_date, instruments_count, status)
        VALUES (%s, %s, %s, %s, %s, 'running')
        RETURNING log_id
    """, (dataset, source, start_date, end_date, instruments_count))
    
    log_id = cursor.fetchone()[0]
    log.info(f"[✔] 创建数据更新日志: {log_id}")
    return log_id


def update_log_success(conn, log_id: int, rows_inserted: int = 0, rows_updated: int = 0):
    """更新日志为成功状态"""
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE data_update_logs SET
            status = 'completed',
            rows_inserted = %s,
            rows_updated = %s,
            completed_at = now(),
            duration_seconds = EXTRACT(EPOCH FROM (now() - started_at))::INT
        WHERE log_id = %s
    """, (rows_inserted, rows_updated, log_id))
    
    log.info(f"[✔] 数据更新完成: log_id={log_id}")


def update_log_failure(conn, log_id: int, error_message: str):
    """更新日志为失败状态"""
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE data_update_logs SET
            status = 'failed',
            error_message = %s,
            completed_at = now(),
            duration_seconds = EXTRACT(EPOCH FROM (now() - started_at))::INT
        WHERE log_id = %s
    """, (error_message, log_id))
    
    log.error(f"[✖] 数据更新失败: log_id={log_id}, error={error_message}")


def get_recent_logs(conn, dataset: str = None, limit: int = 10) -> pd.DataFrame:
    """获取最近的更新日志"""
    query = "SELECT * FROM data_update_logs WHERE 1=1"
    params = []
    
    if dataset:
        query += " AND dataset = %s"
        params.append(dataset)
    
    query += " ORDER BY started_at DESC LIMIT %s"
    params.append(limit)
    
    cursor = conn.cursor()
    cursor.execute(query, params)
    
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)
