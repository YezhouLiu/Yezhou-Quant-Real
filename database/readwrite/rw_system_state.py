from typing import Any
from psycopg.types.json import Jsonb
from utils.logger import get_logger

log = get_logger("rw_system_state")


def set_state(conn, key: str, value: Any):
    """
    设置系统状态（JSONB）

    设计约定：
    - value 接受任意 JSON-serializable Python 对象
    - 统一通过 psycopg Jsonb 适配，避免手写 ::jsonb 出错
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO system_state (key, value, updated_at)
        VALUES (%s, %s, now())
        ON CONFLICT (key)
        DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = now()
        """,
        (key, Jsonb(value)),
    )
    log.info(f"[✔] 设置系统状态: {key}")


def get_state(conn, key: str, default: Any = None) -> Any:
    """
    获取系统状态

    返回：
    - 若存在：Python 原生类型（str / dict / list / int / ...）
    - 若不存在：default
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT value FROM system_state WHERE key = %s",
        (key,),
    )

    row = cursor.fetchone()
    if not row:
        return default

    return row[0]


def delete_state(conn, key: str):
    """删除系统状态"""
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM system_state WHERE key = %s",
        (key,),
    )
    log.info(f"[✔] 删除系统状态: {key}")


def get_all_states(conn) -> dict:
    """获取所有系统状态（key -> Python value）"""
    cursor = conn.cursor()
    cursor.execute("SELECT key, value FROM system_state")

    return {key: value for key, value in cursor.fetchall()}
