from utils.logger import get_logger

log = get_logger("database")


def create_instruments_table(conn, if_exists='skip'):
    """创建资产主表"""
    
    if if_exists == 'drop':
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS instruments CASCADE;")
        log.info("[✔] 已删除旧表 instruments")
    
    statement = """
        CREATE TABLE IF NOT EXISTS instruments (
            instrument_id BIGSERIAL PRIMARY KEY,
            
            ticker TEXT NOT NULL,
            exchange TEXT NOT NULL DEFAULT 'US',
            asset_type TEXT NOT NULL DEFAULT 'Stock',
            currency TEXT NOT NULL DEFAULT 'USD',
            
            company_name TEXT,
            description TEXT,
            sector TEXT,
            industry TEXT,
            ipo_date DATE,
            delist_date DATE,
            
            status TEXT NOT NULL DEFAULT 'active',
            is_tradable BOOLEAN DEFAULT FALSE,
            
            created_at TIMESTAMPTZ DEFAULT now(),
            updated_at TIMESTAMPTZ DEFAULT now(),
            
            UNIQUE(ticker, exchange),
            CHECK (status IN ('active','delisted','suspended','bankrupt')),
            CHECK (asset_type IN ('Stock','ETF','Cash'))
        );
        
        COMMENT ON TABLE instruments IS 'V2.01: 资产主表，统一管理 Stock/ETF/Cash';
        COMMENT ON COLUMN instruments.instrument_id IS '稳定主键，解决 ticker 改名问题';
        COMMENT ON COLUMN instruments.is_tradable IS '是否在交易池中（从 universe 同步）';
    """
    
    cursor = conn.cursor()
    cursor.execute(statement)
    log.info("[✔] 表 'instruments' 创建成功")


def create_instruments_indexes(conn):
    """创建索引"""
    
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_instruments_ticker ON instruments(ticker);",
        "CREATE INDEX IF NOT EXISTS idx_instruments_ticker_exchange ON instruments(ticker, exchange);",
        "CREATE INDEX IF NOT EXISTS idx_instruments_tradable ON instruments(is_tradable) WHERE is_tradable = TRUE;",
        "CREATE INDEX IF NOT EXISTS idx_instruments_status ON instruments(status);",
    ]
    
    cursor = conn.cursor()
    for statement in index_statements:
        cursor.execute(statement)
        log.info(f"[✔] 索引创建成功: {statement[:60]}...")
