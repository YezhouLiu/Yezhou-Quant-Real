from utils.logger import get_logger

log = get_logger("database")


def create_data_update_logs_table(conn, if_exists='skip'):
    """创建数据更新日志表"""
    
    if if_exists == 'drop':
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS data_update_logs CASCADE;")
        log.info("[✔] 已删除旧表 data_update_logs")
    
    statement = """
        CREATE TABLE IF NOT EXISTS data_update_logs (
            log_id BIGSERIAL PRIMARY KEY,
            dataset TEXT NOT NULL,
            source TEXT NOT NULL,
            
            start_date DATE,
            end_date DATE,
            instruments_count INT,
            rows_inserted INT,
            rows_updated INT,
            
            status TEXT NOT NULL DEFAULT 'running',
            error_message TEXT,
            
            started_at TIMESTAMPTZ DEFAULT now(),
            completed_at TIMESTAMPTZ,
            duration_seconds INT,
            
            CHECK (status IN ('running','completed','failed','partial')),
            CHECK (dataset IN ('market_prices','fundamental_data','universe','instruments'))
        );
        
        COMMENT ON TABLE data_update_logs IS '数据更新日志（监控 Tiingo API 调用）';
    """
    
    cursor = conn.cursor()
    cursor.execute(statement)
    log.info("[✔] 表 'data_update_logs' 创建成功")


def create_data_update_logs_indexes(conn):
    """创建索引"""
    
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_update_logs_dataset ON data_update_logs(dataset, started_at DESC);",
        "CREATE INDEX IF NOT EXISTS idx_update_logs_status ON data_update_logs(status) WHERE status != 'completed';",
    ]
    
    cursor = conn.cursor()
    for statement in index_statements:
        cursor.execute(statement)
