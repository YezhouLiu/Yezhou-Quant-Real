from utils.logger import get_logger

log = get_logger("database")


def create_instrument_identifiers_table(conn, if_exists='skip'):
    """创建跨源映射表"""
    
    if if_exists == 'drop':
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS instrument_identifiers CASCADE;")
        log.info("[✔] 已删除旧表 instrument_identifiers")
    
    statement = """
        CREATE TABLE IF NOT EXISTS instrument_identifiers (
            instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
            id_type TEXT NOT NULL,
            id_value TEXT NOT NULL,
            valid_from DATE,
            valid_to DATE,
            created_at TIMESTAMPTZ DEFAULT now(),
            
            PRIMARY KEY (id_type, id_value, instrument_id),
            CHECK (id_type IN ('tiingo','yfinance','cusip','isin','sedol','figi'))
        );
        
        COMMENT ON TABLE instrument_identifiers IS 'V2.01: 跨数据源映射（Tiingo/YF/CUSIP）';
    """
    
    cursor = conn.cursor()
    cursor.execute(statement)
    log.info("[✔] 表 'instrument_identifiers' 创建成功")


def create_instrument_identifiers_indexes(conn):
    """创建索引"""
    
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_identifiers_instrument ON instrument_identifiers(instrument_id);",
        "CREATE INDEX IF NOT EXISTS idx_identifiers_type_value ON instrument_identifiers(id_type, id_value);",
    ]
    
    cursor = conn.cursor()
    for statement in index_statements:
        cursor.execute(statement)
