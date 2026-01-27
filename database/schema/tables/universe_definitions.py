from utils.logger import get_logger

log = get_logger("database")


def create_universe_definitions_table(conn, if_exists='skip'):
    """创建 Universe 定义表"""
    
    if if_exists == 'drop':
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS universe_definitions CASCADE;")
        log.info("[✔] 已删除旧表 universe_definitions")
    
    statement = """
        CREATE TABLE IF NOT EXISTS universe_definitions (
            universe_id SERIAL PRIMARY KEY,
            universe_key TEXT NOT NULL UNIQUE,
            display_name TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_ref TEXT,
            created_at TIMESTAMPTZ DEFAULT now(),
            
            CHECK (source_type IN ('wikipedia','manual','api','file_import'))
        );
        
        COMMENT ON TABLE universe_definitions IS 'Universe 定义（SP500/NASDAQ100/自定义）';
    """
    
    cursor = conn.cursor()
    cursor.execute(statement)
    log.info("[✔] 表 'universe_definitions' 创建成功")


def create_universe_definitions_indexes(conn):
    pass
