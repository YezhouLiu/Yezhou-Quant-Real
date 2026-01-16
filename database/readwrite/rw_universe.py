from typing import List, Dict, Optional
import pandas as pd
from utils.logger import get_logger

log = get_logger("rw_universe")


def insert_universe_definition(conn, universe_key: str, display_name: str,
                               source_type: str, source_ref: str = None) -> int:
    """插入 Universe 定义"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO universe_definitions (universe_key, display_name, source_type, source_ref)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (universe_key) DO UPDATE SET
            display_name = EXCLUDED.display_name,
            source_type = EXCLUDED.source_type,
            source_ref = EXCLUDED.source_ref
        RETURNING universe_id
    """, (universe_key, display_name, source_type, source_ref))
    
    universe_id = cursor.fetchone()[0]
    log.info(f"[✔] 插入 Universe 定义: {universe_key} (ID: {universe_id})")
    return universe_id


def create_universe_snapshot(conn, universe_id: int, as_of_date: str,
                             file_path: str = None, raw_content: str = None,
                             notes: str = None) -> int:
    """创建 Universe 快照"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO universe_snapshots (universe_id, as_of_date, file_path, raw_content, notes)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (universe_id, as_of_date) DO UPDATE SET
            file_path = EXCLUDED.file_path,
            raw_content = EXCLUDED.raw_content,
            notes = EXCLUDED.notes,
            captured_at = now()
        RETURNING snapshot_id
    """, (universe_id, as_of_date, file_path, raw_content, notes))
    
    snapshot_id = cursor.fetchone()[0]
    log.info(f"[✔] 创建 Universe 快照: {snapshot_id}")
    return snapshot_id


def add_universe_members(conn, snapshot_id: int, members: List[Dict]):
    """添加 Universe 成员"""
    cursor = conn.cursor()
    
    for member in members:
        cursor.execute("""
            INSERT INTO universe_members (snapshot_id, instrument_id, weight_hint, raw_ticker)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (snapshot_id, instrument_id) DO UPDATE SET
                weight_hint = EXCLUDED.weight_hint
        """, (snapshot_id, member['instrument_id'], member.get('weight_hint'), member.get('raw_ticker')))
    
    cursor.execute("UPDATE universe_snapshots SET row_count = %s WHERE snapshot_id = %s",
                  (len(members), snapshot_id))
    
    log.info(f"[✔] 添加 {len(members)} 个成员到快照 {snapshot_id}")


def get_universe_id(conn, universe_key: str) -> Optional[int]:
    """获取 Universe ID"""
    cursor = conn.cursor()
    cursor.execute("SELECT universe_id FROM universe_definitions WHERE universe_key = %s", (universe_key,))
    result = cursor.fetchone()
    return result[0] if result else None


def get_latest_snapshot(conn, universe_id: int) -> Optional[Dict]:
    """获取最新快照"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM universe_snapshots
        WHERE universe_id = %s
        ORDER BY as_of_date DESC
        LIMIT 1
    """, (universe_id,))
    
    row = cursor.fetchone()
    if not row:
        return None
    
    columns = [desc[0] for desc in cursor.description]
    return dict(zip(columns, row))


def get_snapshot_members(conn, snapshot_id: int) -> pd.DataFrame:
    """获取快照成员"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT um.*, i.ticker, i.company_name, i.sector
        FROM universe_members um
        JOIN instruments i ON um.instrument_id = i.instrument_id
        WHERE um.snapshot_id = %s
        ORDER BY i.ticker
    """, (snapshot_id,))
    
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def delete_universe(conn, universe_id: int):
    """删除 Universe（级联删除快照和成员）"""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM universe_definitions WHERE universe_id = %s", (universe_id,))
    log.warning(f"[⚠] 删除 Universe ID: {universe_id}")
