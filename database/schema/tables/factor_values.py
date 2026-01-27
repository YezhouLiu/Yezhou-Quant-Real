from utils.logger import get_logger

log = get_logger("database")


def create_factor_values_table(conn, if_exists: str = "skip"):
    """
    创建标量因子表（Scalar Factor Values）

    设计原则：
    - 一行 = instrument + date + factor_name + version → 一个数值
    - 所有参数、口径、预处理信息放 JSONB
    """

    if if_exists == "drop":
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS factor_values CASCADE;")
        log.info("[✔] 已删除旧表 factor_values")

    statement = """
        CREATE TABLE IF NOT EXISTS factor_values (
            instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
            date DATE NOT NULL,
            factor_name TEXT NOT NULL,

            factor_value NUMERIC(38,10) NOT NULL,

            -- 因子参数（lookback, skip, window, half_life 等）
            factor_args JSONB,

            -- 口径 / 预处理配置（winsor, zscore, universe, price_field 等）
            config JSONB,

            -- 因子版本，用于重算与并存
            factor_version TEXT NOT NULL DEFAULT 'v1',

            data_source TEXT NOT NULL DEFAULT 'internal',
            ingested_at TIMESTAMPTZ DEFAULT now(),

            PRIMARY KEY (instrument_id, date, factor_name, factor_version)
        );

        COMMENT ON TABLE factor_values IS
            '标量因子值表（instrument × date × factor × version → scalar）';

        COMMENT ON COLUMN factor_values.factor_name IS
            '因子名称，如 mom_252d_21d_skip, vol_60d, adv_20d';

        COMMENT ON COLUMN factor_values.factor_value IS
            '因子数值（标量）';

        COMMENT ON COLUMN factor_values.factor_args IS
            '因子参数（JSONB），如 lookback / skip / window';

        COMMENT ON COLUMN factor_values.config IS
            '预处理与口径配置（JSONB），如 winsor / zscore / universe';
    """

    cursor = conn.cursor()
    cursor.execute(statement)
    log.info("[✔] 表 'factor_values' 创建成功")


def create_factor_values_indexes(conn):
    """创建 factor_values 相关索引"""

    index_statements = [
        # 某因子某天的截面（选股 / IC / 分组）
        """
        CREATE INDEX IF NOT EXISTS idx_factor_values_name_date_ver
        ON factor_values (factor_name, date, factor_version);
        """,

        # 单标的因子时间序列
        """
        CREATE INDEX IF NOT EXISTS idx_factor_values_instrument_date
        ON factor_values (instrument_id, date);
        """,

        # 某天取全部因子（构建训练集 / 回测）
        """
        CREATE INDEX IF NOT EXISTS idx_factor_values_date
        ON factor_values (date);
        """
    ]

    cursor = conn.cursor()
    for stmt in index_statements:
        cursor.execute(stmt)

    log.info("[✔] factor_values 索引创建完成")
