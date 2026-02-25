from database.readwrite.rw_market_prices import get_price_max_date
from database.utils.db_utils import get_db_connection
from utils.logger import get_logger

log = get_logger("update_tradable_universe")


def update_tradable_universe(
    *,
    min_price: float = 5.0,
    min_avg_dollar_volume: float = 1_000_000,
    lookback_days: int = 60,
):
    """
    每日更新可交易标的逻辑：

    1️⃣ 股票：
        - 最近 lookback_days 平均成交额 >= min_avg_dollar_volume
        - 最新价格 >= min_price

    2️⃣ ETF：
        - 永远可交易

    不做容错，缺数据直接炸。
    """

    conn = get_db_connection()
    cursor = conn.cursor()

    asof_date = get_price_max_date(conn)
    if not asof_date:
        raise ValueError("market_prices empty, cannot update universe")

    # 1️⃣ 取最近 N 个交易日
    cursor.execute(
        """
        SELECT date
        FROM trading_calendar
        WHERE date <= %s
        ORDER BY date DESC
        LIMIT %s
        """,
        (asof_date, lookback_days),
    )

    rows = cursor.fetchall()
    if len(rows) < lookback_days:
        raise ValueError("not enough trading days for lookback window")

    lookback_start = rows[-1][0]
    lookback_end = rows[0][0]

    # 2️⃣ 计算符合条件的股票
    cursor.execute(
        """
        WITH recent_data AS (
            SELECT
                instrument_id,
                AVG(adj_close * volume) AS avg_dollar_vol,
                MAX(adj_close) FILTER (WHERE date = %s) AS last_price
            FROM market_prices
            WHERE date >= %s
              AND date <= %s
            GROUP BY instrument_id
        )
        SELECT i.instrument_id
        FROM recent_data r
        JOIN instruments i ON r.instrument_id = i.instrument_id
        WHERE i.asset_type = 'Stock'
          AND r.avg_dollar_vol >= %s
          AND r.last_price >= %s
        """,
        (
            asof_date,
            lookback_start,
            lookback_end,
            min_avg_dollar_volume,
            min_price,
        ),
    )

    tradable_stock_ids = [r[0] for r in cursor.fetchall()]

    # 3️⃣ 全部设为 false
    cursor.execute(
        """
        UPDATE instruments
        SET is_tradable = false
        """
    )

    # 4️⃣ 股票符合规则的设为 true
    if tradable_stock_ids:
        cursor.execute(
            """
            UPDATE instruments
            SET is_tradable = true
            WHERE instrument_id = ANY(%s)
            """,
            (tradable_stock_ids,),
        )

    # 5️⃣ ETF 永远可交易
    cursor.execute(
        """
        UPDATE instruments
        SET is_tradable = true
        WHERE asset_type = 'ETF'
        """
    )

    conn.commit()
    conn.close()

    log.info(
        f"[✔] Tradable universe updated | "
        f"stocks={len(tradable_stock_ids)} | ETFs forced tradable"
    )
