from utils.logger import get_logger

log = get_logger("database")


def create_universe_snapshots_table(conn, if_exists='skip'):
    """创建 Universe 快照表"""
    
    if if_exists == 'drop':
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS universe_snapshots CASCADE;")
        log.info("[✔] 已删除旧表 universe_snapshots")
    
    statement = """
        CREATE TABLE IF NOT EXISTS universe_snapshots (
            snapshot_id BIGSERIAL PRIMARY KEY,
            universe_id INT NOT NULL REFERENCES universe_definitions(universe_id) ON DELETE CASCADE,
            as_of_date DATE NOT NULL,
            captured_at TIMESTAMPTZ DEFAULT now(),
            
            row_count INT,
            file_path TEXT,
            raw_content TEXT,
            notes TEXT,
            
            UNIQUE(universe_id, as_of_date)
        );
        
        COMMENT ON TABLE universe_snapshots IS 'Universe 版本快照（防爬虫失效）';
        COMMENT ON COLUMN universe_snapshots.file_path IS '本地 CSV 备份路径';
        COMMENT ON COLUMN universe_snapshots.raw_content IS '原始 HTML/CSV 内容（可重解析）';
    """
    
    cursor = conn.cursor()
    cursor.execute(statement)
    log.info("[✔] 表 'universe_snapshots' 创建成功")


def create_universe_snapshots_indexes(conn):
    pass
