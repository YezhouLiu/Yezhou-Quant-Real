from typing import List, Dict, Optional
from utils.logger import get_logger

log = get_logger("rw_instrument_identifiers")


def insert_identifier(conn, instrument_id: int, source: str, source_id: str,
                     additional_info: dict = None):
    """插入资产标识符"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO instrument_identifiers (instrument_id, source, source_id, additional_info)
        VALUES (%s, %s, %s, %s::jsonb)
        ON CONFLICT (instrument_id, source) DO UPDATE SET
            source_id = EXCLUDED.source_id,
            additional_info = EXCLUDED.additional_info,
            updated_at = now()
    """, (instrument_id, source, source_id, additional_info))
    
    log.info(f"[✔] 插入标识符: instrument_id={instrument_id}, source={source}")


def get_identifier(conn, instrument_id: int, source: str) -> Optional[str]:
    """获取资产在指定数据源的标识符"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT source_id FROM instrument_identifiers
        WHERE instrument_id = %s AND source = %s
    """, (instrument_id, source))
    
    result = cursor.fetchone()
    return result[0] if result else None


def get_instrument_by_source_id(conn, source: str, source_id: str) -> Optional[int]:
    """根据数据源ID查找资产"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT instrument_id FROM instrument_identifiers
        WHERE source = %s AND source_id = %s
    """, (source, source_id))
    
    result = cursor.fetchone()
    return result[0] if result else None


def get_all_identifiers(conn, instrument_id: int) -> Dict[str, str]:
    """获取资产的所有标识符"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT source, source_id FROM instrument_identifiers
        WHERE instrument_id = %s
    """, (instrument_id,))
    
    return {row[0]: row[1] for row in cursor.fetchall()}


def batch_insert_identifiers(conn, identifiers: List[Dict]):
    """批量插入标识符"""
    cursor = conn.cursor()
    
    for ident in identifiers:
        cursor.execute("""
            INSERT INTO instrument_identifiers (instrument_id, source, source_id, additional_info)
            VALUES (%s, %s, %s, %s::jsonb)
            ON CONFLICT (instrument_id, source) DO UPDATE SET
                source_id = EXCLUDED.source_id,
                updated_at = now()
        """, (
            ident['instrument_id'], ident['source'], ident['source_id'],
            ident.get('additional_info')
        ))
    
    log.info(f"[✔] 批量插入 {len(identifiers)} 个标识符")


def delete_identifier(conn, instrument_id: int, source: str):
    """删除标识符"""
    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM instrument_identifiers
        WHERE instrument_id = %s AND source = %s
    """, (instrument_id, source))
    
    log.info(f"[✔] 删除标识符: instrument_id={instrument_id}, source={source}")
