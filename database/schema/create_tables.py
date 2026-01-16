from database.utils.db_utils import get_db_connection
from utils.logger import get_logger

# 导入各表的创建函数
from database.schema.tables.instruments import (
    create_instruments_table,
    create_instruments_indexes
)
from database.schema.tables.instrument_identifiers import (
    create_instrument_identifiers_table,
    create_instrument_identifiers_indexes
)
from database.schema.tables.market_prices import (
    create_market_prices_table,
    create_market_prices_indexes
)
from database.schema.tables.fundamental_data import (
    create_fundamental_data_table,
    create_fundamental_data_indexes
)
from database.schema.tables.universe_definitions import (
    create_universe_definitions_table,
    create_universe_definitions_indexes
)
from database.schema.tables.universe_snapshots import (
    create_universe_snapshots_table,
    create_universe_snapshots_indexes
)
from database.schema.tables.universe_members import (
    create_universe_members_table,
    create_universe_members_indexes
)
from database.schema.tables.trading_calendar import (
    create_trading_calendar_table,
    create_trading_calendar_indexes
)
from database.schema.tables.fills import (
    create_fills_table,
    create_fills_indexes
)
from database.schema.tables.positions import (
    create_positions_table,
    create_positions_indexes
)
from database.schema.tables.system_state import (
    create_system_state_table,
    create_system_state_indexes
)
from database.schema.tables.data_update_logs import (
    create_data_update_logs_table,
    create_data_update_logs_indexes
)

log = get_logger("database")


def create_all_tables(conn, if_exists='skip'):
    """
    创建所有数据库表
    
    Args:
        conn: 数据库连接
        if_exists: 表已存在时的处理方式 ('skip'=跳过, 'drop'=删除重建)
    """
    create_instruments_table(conn, if_exists)
    create_instrument_identifiers_table(conn, if_exists)
    create_market_prices_table(conn, if_exists)
    create_fundamental_data_table(conn, if_exists)
    
    create_universe_definitions_table(conn, if_exists)
    create_universe_snapshots_table(conn, if_exists)
    create_universe_members_table(conn, if_exists)
    
    create_trading_calendar_table(conn, if_exists)
    
    create_fills_table(conn, if_exists)
    create_positions_table(conn, if_exists)
    
    create_system_state_table(conn, if_exists)
    create_data_update_logs_table(conn, if_exists)
    
    print("\n✅ 所有表创建完毕")


def create_indexes(conn):
    """创建所有索引"""
    
    create_instruments_indexes(conn)
    create_instrument_identifiers_indexes(conn)
    create_market_prices_indexes(conn)
    create_fundamental_data_indexes(conn)
    create_universe_definitions_indexes(conn)
    create_universe_snapshots_indexes(conn)
    create_universe_members_indexes(conn)
    create_trading_calendar_indexes(conn)
    create_fills_indexes(conn)
    create_positions_indexes(conn)
    create_system_state_indexes(conn)
    create_data_update_logs_indexes(conn)
    
    print("✅ 所有索引创建完毕")


# ============================================================================
# 主执行函数
# ============================================================================
if __name__ == "__main__":
    print("\n" + "="*70)
    print("🚀 开始创建数据库表结构")
    print("="*70 + "\n")
    
    conn = None
    try:
        conn = get_db_connection()
        
        create_all_tables(conn, if_exists='drop')
        create_indexes(conn)
        
        conn.commit()
        
        print("\n" + "="*70)
        print("✅ 数据库表结构创建完成")
        print("="*70 + "\n")
        
    except Exception as e:
        if conn:
            conn.rollback()
        log.error(f"[✖] 数据库初始化失败: {e}")
        raise
    finally:
        if conn:
            conn.close()

