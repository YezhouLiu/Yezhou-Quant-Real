from utils.logger import get_logger

log = get_logger("database")


def create_fundamental_data_table(conn, if_exists='skip'):
    """创建基本面数据表"""
    
    if if_exists == 'drop':
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS fundamental_data CASCADE;")
        log.info("[✔] 已删除旧表 fundamental_data")
    
    statement = """
        CREATE TABLE IF NOT EXISTS fundamental_data (
            instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
            report_date DATE NOT NULL,
            metric_name TEXT NOT NULL,
            
            metric_value NUMERIC(20,6),
            period_type TEXT NOT NULL,
            period_start DATE,
            period_end DATE,
            
            currency TEXT DEFAULT 'USD',
            data_source TEXT DEFAULT 'tiingo',
            ingested_at TIMESTAMPTZ DEFAULT now(),
            
            PRIMARY KEY (instrument_id, report_date, metric_name, period_type),
            CHECK (period_type IN ('TTM','Quarterly','Annual'))
        );
        
        COMMENT ON TABLE fundamental_data IS 'V2.01: 基本面数据（Tiingo Fundamentals - 预留但暂不使用）';
        COMMENT ON COLUMN fundamental_data.metric_name IS 'EPS, Revenue, NetIncome, ROE, PE, PB...';
    """
    
    cursor = conn.cursor()
    cursor.execute(statement)
    log.info("[✔] 表 'fundamental_data' 创建成功")


def create_fundamental_data_indexes(conn):
    """创建索引"""
    
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_fundamental_instrument_date ON fundamental_data(instrument_id, report_date);",
        "CREATE INDEX IF NOT EXISTS idx_fundamental_metric ON fundamental_data(metric_name);",
    ]
    
    cursor = conn.cursor()
    for statement in index_statements:
        cursor.execute(statement)
