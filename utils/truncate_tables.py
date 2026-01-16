"""
数据库表清空工具集
==================

提供各种truncate函数，用于清空数据库表。

使用方法：
    from utils.truncate_tables import truncate_all_tables
    truncate_all_tables()

或命令行：
    python -c "from utils.truncate_tables import truncate_all_tables; truncate_all_tables()"
"""
from database.utils.db_utils import get_db_connection
from utils.logger import get_logger

log = get_logger("truncate_tables")


# ============================================================================
# 清空所有表（完整重置）
# ============================================================================
def truncate_all_tables():
    """清空所有数据库表并重置序列

    警告：这会删除所有数据！仅用于开发/测试环境。

    顺序：按照外键依赖的反向顺序清空（子表先清空）
    RESTART IDENTITY：重置自增序列（instrument_id等从1开始）
    CASCADE：级联清空有外键关系的表
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # 按照反向依赖顺序清空（从最外层到核心表）
    tables_to_truncate = [
        # 第5层：系统管理
        "data_update_logs",
        "system_state",

        # 第4层：实盘台账
        "live_positions_daily",
        "live_fills",

        # 第3层：交易日历
        "trading_calendar",

        # 第2层：Universe管理
        "universe_members",
        "universe_snapshots",
        "universe_definitions",

        # 第1层：核心数据（有外键依赖的表）
        "fundamental_data",
        "market_prices",
        "instrument_identifiers",
        "instruments",  # 最后清空主表
    ]

    try:
        log.info("=" * 70)
        log.info("开始清空所有数据库表...")
        log.info("=" * 70)

        for table_name in tables_to_truncate:
            try:
                # RESTART IDENTITY 会重置序列
                # CASCADE 会级联清空相关表
                cursor.execute(
                    f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
                log.info(f"✅ {table_name}")
            except Exception as e:
                log.warning(f"⚠️  {table_name}: {e}")

        conn.commit()

        log.info("=" * 70)
        log.info("✅ 所有表格已清空，序列已重置！")
        log.info("=" * 70)

    except Exception as e:
        conn.rollback()
        log.error(f"清空表格失败: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


# ============================================================================
# 清空实验回测相关表（保留历史数据）
# ============================================================================
def truncate_experimental_tables():
    """清空实验回测相关的表（不影响实盘数据）

    包括：
    - experimental_rebalance_records
    - experimental_position_history
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE experimental_rebalance_records RESTART IDENTITY CASCADE;")
            cursor.execute(
                "TRUNCATE TABLE experimental_position_history RESTART IDENTITY CASCADE;")
        conn.commit()

    log.info("✅ 已清空实验回测表（experimental_*）")


# ----------------------------------------------------------------------------------------------------------------------------------------
# 清空 experimental_rebalance_records（兼容旧函数名）
# ----------------------------------------------------------------------------------------------------------------------------------------
def truncate_experimental_rebalance_record():
    """清空实验调仓记录表"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE experimental_rebalance_records RESTART IDENTITY;")
        conn.commit()
    log.info("✅ 已清空 experimental_rebalance_records 表")


# ----------------------------------------------------------------------------------------------------------------------------------------
# 清空 experimental_position_history（兼容旧函数名）
# ----------------------------------------------------------------------------------------------------------------------------------------
def truncate_experimental_position_history():
    """清空实验持仓历史表"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE experimental_position_history RESTART IDENTITY;")
        conn.commit()
    log.info("✅ 已清空 experimental_position_history 表")
