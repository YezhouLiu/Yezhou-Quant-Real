from utils.logger import get_logger

log = get_logger("database")


def create_fills_table(conn, if_exists='skip'):
    """创建成交记录表"""
    
    if if_exists == 'drop':
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS fills CASCADE;")
        log.info("[✔] 已删除旧表 fills")
    
    statement = """
        CREATE TABLE IF NOT EXISTS fills (
            fill_id BIGSERIAL PRIMARY KEY,
            instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
            side TEXT NOT NULL,
            
            quantity NUMERIC(20,8) NOT NULL,
            price NUMERIC(20,6) NOT NULL,
            trade_time TIMESTAMPTZ NOT NULL,
            
            commission NUMERIC(20,6) DEFAULT 0,
            fees NUMERIC(20,6) DEFAULT 0,
            fx_rate NUMERIC(20,8),
            
            notes TEXT,
            source TEXT DEFAULT 'manual',
            created_at TIMESTAMPTZ DEFAULT now(),
            
            CHECK (side IN ('BUY','SELL')),
            CHECK (source IN ('manual','ibkr','csv_import','api'))
        );
        
        COMMENT ON TABLE fills IS '成交记录';
        COMMENT ON COLUMN fills.trade_time IS '成交时间';
    """
    
    cursor = conn.cursor()
    cursor.execute(statement)
    log.info("[✔] 表 'fills' 创建成功")


def create_fills_indexes(conn):
    """创建索引"""
    
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_fills_instrument_time ON fills(instrument_id, trade_time DESC);",
        "CREATE INDEX IF NOT EXISTS idx_fills_trade_time ON fills(trade_time DESC);",
    ]
    
    cursor = conn.cursor()
    for statement in index_statements:
        cursor.execute(statement)
