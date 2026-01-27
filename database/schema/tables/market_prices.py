from utils.logger import get_logger

log = get_logger("database")


def create_market_prices_table(conn, if_exists='skip'):
    """创建市场价格表"""
    
    if if_exists == 'drop':
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS market_prices CASCADE;")
        log.info("[✔] 已删除旧表 market_prices")
    
    statement = """
        CREATE TABLE IF NOT EXISTS market_prices (
            instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
            date DATE NOT NULL,
            
            open_price NUMERIC(20,6),
            high_price NUMERIC(20,6),
            low_price NUMERIC(20,6),
            close_price NUMERIC(20,6) NOT NULL,
            volume BIGINT,
            
            adj_open NUMERIC(20,6),
            adj_high NUMERIC(20,6),
            adj_low NUMERIC(20,6),
            adj_close NUMERIC(20,6) NOT NULL,
            adj_volume BIGINT,
            
            dividends NUMERIC(20,6) DEFAULT 0,
            stock_splits NUMERIC(20,6) DEFAULT 1,
            
            data_source TEXT NOT NULL DEFAULT 'tiingo',
            ingested_at TIMESTAMPTZ DEFAULT now(),
            
            PRIMARY KEY (instrument_id, date)
        );
        
        COMMENT ON TABLE market_prices IS 'Tiingo EOD 价格（完整 OHLCV + 复权）';
        COMMENT ON COLUMN market_prices.dividends IS '当日分红（美元）';
        COMMENT ON COLUMN market_prices.stock_splits IS '拆股因子（2.0=1拆2，0.5=2合1）';
    """
    
    cursor = conn.cursor()
    cursor.execute(statement)
    log.info("[✔] 表 'market_prices' 创建成功")


def create_market_prices_indexes(conn):
    """创建索引"""
    
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_prices_date ON market_prices(date);",
        "CREATE INDEX IF NOT EXISTS idx_prices_instrument_date ON market_prices(instrument_id, date);",
    ]
    
    cursor = conn.cursor()
    for statement in index_statements:
        cursor.execute(statement)
