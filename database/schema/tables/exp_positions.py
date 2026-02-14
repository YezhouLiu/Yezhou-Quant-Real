from utils.logger import get_logger

log = get_logger("database")


def create_exp_positions_table(conn, if_exists="skip"):
    """创建实验持仓表（极简展示用）"""

    if if_exists == "drop":
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS exp_positions CASCADE;")
        log.info("[✔] 已删除旧表 exp_positions")

    statement = """
        CREATE TABLE IF NOT EXISTS exp_positions (
            date DATE NOT NULL,
            instrument_id BIGINT NOT NULL 
                REFERENCES instruments(instrument_id) ON DELETE CASCADE,
            
            quantity NUMERIC(20,8) NOT NULL,        -- 持仓数量（CASH 是数量）
            buy_price NUMERIC(20,6),                -- 买入价格（CASH 为1）
            current_price NUMERIC(20,6),            -- 当前价格（CASH 为1）
            market_value NUMERIC(20,6) NOT NULL,    -- 市值（quantity * current_price）
            
            PRIMARY KEY (date, instrument_id)
        );
        
        COMMENT ON TABLE exp_positions IS '实验用持仓快照表（按日期记录）';
        COMMENT ON COLUMN exp_positions.market_value IS '该行对应资产的市值';
    """

    cursor = conn.cursor()
    cursor.execute(statement)
    log.info("[✔] 表 'exp_positions' 创建成功")


def create_exp_positions_indexes(conn):
    """创建索引"""

    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_exp_positions_date ON exp_positions(date DESC);",
        "CREATE INDEX IF NOT EXISTS idx_exp_positions_instrument ON exp_positions(instrument_id);",
    ]

    cursor = conn.cursor()
    for statement in index_statements:
        cursor.execute(statement)
