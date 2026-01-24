from utils.logger import get_logger

log = get_logger("database")


def create_fundamental_daily_table(conn, if_exists: str = "skip"):
    """
    创建每日基本面/估值类数据表（点位数据）
    - 用于 MarketCap, PE, PB, EV, PS, shares, float 等每日变化或按日对齐的指标
    - 不与财报期 fundamental_data 混用，保持语义干净
    """

    if if_exists == "drop":
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS fundamental_daily CASCADE;")
        log.info("[✔] 已删除旧表 fundamental_daily")

    statement = """
        CREATE TABLE IF NOT EXISTS fundamental_daily (
            instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
            date DATE NOT NULL,
            metric_name TEXT NOT NULL,

            metric_value NUMERIC(20,6),
            currency TEXT DEFAULT 'USD',
            data_source TEXT DEFAULT 'tiingo',
            ingested_at TIMESTAMPTZ DEFAULT now(),

            PRIMARY KEY (instrument_id, date, metric_name)
        );

        COMMENT ON TABLE fundamental_daily IS 'V2.01: 每日基本面/估值点位数据（如 MarketCap, PE_TTM, PB, EV, Shares 等）';
        COMMENT ON COLUMN fundamental_daily.metric_name IS 'MarketCap, PE_TTM, PB, EV, PS, SharesOutstanding...';

        -- 高频查询优化：按指标跨标的取时间序列
        CREATE INDEX IF NOT EXISTS idx_fund_daily_metric_date
            ON fundamental_daily (metric_name, date);

        -- 常见查询：单标的按时间取所有指标
        CREATE INDEX IF NOT EXISTS idx_fund_daily_instrument_date
            ON fundamental_daily (instrument_id, date);
    """

    cursor = conn.cursor()
    cursor.execute(statement)
    log.info("[✔] 表 'fundamental_daily' 创建成功")
