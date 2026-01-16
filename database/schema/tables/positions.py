from utils.logger import get_logger

log = get_logger("database")


def create_positions_table(conn, if_exists='skip'):
    """创建持仓快照表"""
    
    if if_exists == 'drop':
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS positions CASCADE;")
        log.info("[✔] 已删除旧表 positions")
    
    statement = """
        CREATE TABLE IF NOT EXISTS positions (
            date DATE NOT NULL,
            instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
            
            quantity NUMERIC(20,8) NOT NULL,
            cost_basis NUMERIC(20,6),
            last_price NUMERIC(20,6),
            market_value NUMERIC(20,6),
            
            updated_at TIMESTAMPTZ DEFAULT now(),
            source TEXT DEFAULT 'computed',
            
            PRIMARY KEY (date, instrument_id),
            CHECK (source IN ('computed','manual_adjust'))
        );
        
        COMMENT ON TABLE positions IS '持仓快照';
        COMMENT ON COLUMN positions.last_price IS '估值价格';
    """
    
    cursor = conn.cursor()
    cursor.execute(statement)
    log.info("[✔] 表 'positions' 创建成功")


def create_positions_indexes(conn):
    """创建索引"""
    
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_positions_date ON positions(date);",
    ]
    
    cursor = conn.cursor()
    for statement in index_statements:
        cursor.execute(statement)
