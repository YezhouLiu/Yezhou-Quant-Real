from typing import Optional
import pandas as pd
from utils.logger import get_logger

log = get_logger("rw_universe_snapshots")


def get_snapshot_by_id(conn, snapshot_id: int) -> Optional[dict]:
    """根据ID获取快照"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM universe_snapshots WHERE snapshot_id = %s
    """, (snapshot_id,))
    
    row = cursor.fetchone()
    if not row:
        return None
    
    columns = [desc[0] for desc in cursor.description]
    return dict(zip(columns, row))


def get_all_snapshots(conn, universe_id: int) -> pd.DataFrame:
    """获取Universe的所有快照"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM universe_snapshots
        WHERE universe_id = %s
        ORDER BY as_of_date DESC
    """, (universe_id,))
    
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def update_snapshot_notes(conn, snapshot_id: int, notes: str):
    """更新快照备注"""
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE universe_snapshots SET notes = %s
        WHERE snapshot_id = %s
    """, (notes, snapshot_id))
    
    log.info(f"[✔] 更新快照备注: snapshot_id={snapshot_id}")


def delete_snapshot(conn, snapshot_id: int):
    """删除快照（级联删除成员）"""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM universe_snapshots WHERE snapshot_id = %s", (snapshot_id,))
    log.warning(f"[⚠] 删除快照: snapshot_id={snapshot_id}")
