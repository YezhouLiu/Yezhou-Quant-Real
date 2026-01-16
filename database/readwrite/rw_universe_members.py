from typing import Optional
import pandas as pd
from utils.logger import get_logger

log = get_logger("rw_universe_members")


def get_member_count(conn, snapshot_id: int) -> int:
    """获取快照成员数量"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM universe_members WHERE snapshot_id = %s
    """, (snapshot_id,))
    
    return cursor.fetchone()[0]


def is_in_universe(conn, snapshot_id: int, instrument_id: int) -> bool:
    """检查资产是否在Universe中"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT EXISTS(
            SELECT 1 FROM universe_members
            WHERE snapshot_id = %s AND instrument_id = %s
        )
    """, (snapshot_id, instrument_id))
    
    return cursor.fetchone()[0]


def get_member_tickers(conn, snapshot_id: int) -> list:
    """获取快照中所有的ticker"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT i.ticker
        FROM universe_members um
        JOIN instruments i ON um.instrument_id = i.instrument_id
        WHERE um.snapshot_id = %s
        ORDER BY i.ticker
    """, (snapshot_id,))
    
    return [row[0] for row in cursor.fetchall()]


def update_member_weight(conn, snapshot_id: int, instrument_id: int, weight_hint: float):
    """更新成员权重"""
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE universe_members SET weight_hint = %s
        WHERE snapshot_id = %s AND instrument_id = %s
    """, (weight_hint, snapshot_id, instrument_id))
    
    log.info(f"[✔] 更新成员权重: snapshot_id={snapshot_id}, instrument_id={instrument_id}")


def delete_member(conn, snapshot_id: int, instrument_id: int):
    """删除成员"""
    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM universe_members
        WHERE snapshot_id = %s AND instrument_id = %s
    """, (snapshot_id, instrument_id))
    
    log.info(f"[✔] 删除成员: snapshot_id={snapshot_id}, instrument_id={instrument_id}")
