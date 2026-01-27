from utils.logger import get_logger

log = get_logger("database")


def create_corporate_actions_table(conn, if_exists='skip'):
    """创建公司行为数据表（分红 / 拆股 / 合股 等）"""

    if if_exists == 'drop':
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS corporate_actions CASCADE;")
        log.info("[✔] 已删除旧表 corporate_actions")

    statement = """
        CREATE TABLE IF NOT EXISTS corporate_actions (
            instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
            action_date DATE NOT NULL,
            action_type TEXT NOT NULL,
            
            action_value NUMERIC(20,6),
            currency TEXT DEFAULT 'USD',
            
            data_source TEXT DEFAULT 'tiingo',
            raw_payload JSONB,
            ingested_at TIMESTAMPTZ DEFAULT now(),
            
            PRIMARY KEY (instrument_id, action_date, action_type),
            CHECK (action_type IN (
                'DIVIDEND_CASH',
                'SPLIT',
                'REVERSE_SPLIT',
                'SPINOFF',
                'SPECIAL_DIVIDEND'
            ))
        );

        COMMENT ON TABLE corporate_actions IS '公司行为数据（Tiingo Corporate Actions）';
        COMMENT ON COLUMN corporate_actions.action_type IS '分红/拆股/合股等公司行为类型';
        COMMENT ON COLUMN corporate_actions.action_value IS '分红金额（每股）或拆股比例';
        COMMENT ON COLUMN corporate_actions.raw_payload IS 'Tiingo 原始返回数据，用于追溯';
    """

    cursor = conn.cursor()
    cursor.execute(statement)
    log.info("[✔] 表 \'corporate_actions\' 创建成功")


def create_corporate_actions_indexes(conn):
    """创建公司行为索引"""

    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_corp_action_instrument_date ON corporate_actions(instrument_id, action_date);",
        "CREATE INDEX IF NOT EXISTS idx_corp_action_type ON corporate_actions(action_type);",
    ]

    cursor = conn.cursor()
    for statement in index_statements:
        cursor.execute(statement)
