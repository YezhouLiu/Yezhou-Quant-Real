from typing import Any, Optional
import json
from utils.logger import get_logger

log = get_logger("rw_system_state")


def set_state(conn, key: str, value: Any):
    """设置系统状态"""
    cursor = conn.cursor()
    
    if not isinstance(value, str):
        value = json.dumps(value)
    
    cursor.execute("""
        INSERT INTO system_state (key, value)
        VALUES (%s, %s::jsonb)
        ON CONFLICT (key) DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = now()
    """, (key, value))
    
    log.info(f"[✔] 设置系统状态: {key}")


def get_state(conn, key: str, default: Any = None) -> Any:
    """获取系统状态"""
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM system_state WHERE key = %s", (key,))
    
    result = cursor.fetchone()
    if not result:
        return default
    
    return result[0]


def delete_state(conn, key: str):
    """删除系统状态"""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM system_state WHERE key = %s", (key,))
    log.info(f"[✔] 删除系统状态: {key}")


def get_all_states(conn) -> dict:
    """获取所有系统状态"""
    cursor = conn.cursor()
    cursor.execute("SELECT key, value FROM system_state")
    
    return {row[0]: row[1] for row in cursor.fetchall()}
