from utils.logger import get_logger

log = get_logger("database")


def create_universe_members_table(conn, if_exists='skip'):
    """创建 Universe 成员表"""
    
    if if_exists == 'drop':
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS universe_members CASCADE;")
        log.info("[✔] 已删除旧表 universe_members")
    
    statement = """
        CREATE TABLE IF NOT EXISTS universe_members (
            snapshot_id BIGINT NOT NULL REFERENCES universe_snapshots(snapshot_id) ON DELETE CASCADE,
            instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
            weight_hint NUMERIC(10,6),
            raw_ticker TEXT,
            
            PRIMARY KEY (snapshot_id, instrument_id)
        );
        
        COMMENT ON TABLE universe_members IS 'Universe 成员列表';
        COMMENT ON COLUMN universe_members.weight_hint IS '可选：指数权重（如果来源提供）';
    """
    
    cursor = conn.cursor()
    cursor.execute(statement)
    log.info("[✔] 表 'universe_members' 创建成功")


def create_universe_members_indexes(conn):
    """创建索引"""
    
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_universe_members_instrument ON universe_members(instrument_id);",
        "CREATE INDEX IF NOT EXISTS idx_universe_members_snapshot ON universe_members(snapshot_id);",
    ]
    
    cursor = conn.cursor()
    for statement in index_statements:
        cursor.execute(statement)
