from database.utils.db_utils import get_db_connection
from utils.logger import get_logger

log = get_logger("update_tradable_universe")


def update_tradable_universe(
    *,
    tradable_only: bool = False,
    min_price: float = 1.0,
    min_avg_volume: int = 100_000,
    volume_lookback_days: int = 20,
    commit: bool = True,
):
    """
    更新 instruments.is_tradable（不依赖 fundamentals）

    数据源约定：
    - 价格表：market_prices
    - 价格字段：COALESCE(adj_close, close_price)
    - 成交量字段：volume（或 adj_volume 也可，这里用 volume）

    Rules:
    - ETF：始终 tradable
    - Stock：
        - status in ('delisted','bankrupt','suspended') -> FALSE
        - 最新交易日价格 < min_price -> FALSE
        - 近 N 天（相对 market_prices.max(date)）平均成交量 < min_avg_volume -> FALSE
    """

    conn = get_db_connection()
    if not conn:
        log.error("❌ DB connection failed")
        return

    cursor = conn.cursor()

    scope_filter = ""
    if tradable_only:
        scope_filter = "AND i.is_tradable = TRUE"

    # 以 market_prices 的最新日期为锚点（周末/假日也稳定）
    cursor.execute("SELECT MAX(date) FROM market_prices")
    last_px_date = cursor.fetchone()[0]
    if last_px_date is None:
        conn.close()
        raise ValueError("market_prices is empty; cannot update tradable universe")

    log.info("=" * 70)
    log.info("🚦 Updating tradable universe (market-based, market_prices)")
    log.info(f"tradable_only        = {tradable_only}")
    log.info(f"min_price            = {min_price}")
    log.info(f"min_avg_volume       = {min_avg_volume}")
    log.info(f"volume_lookback_days = {volume_lookback_days}")
    log.info(f"price_anchor_date    = {last_px_date}")
    log.info(f"commit               = {commit}")
    log.info("=" * 70)

    try:
        # ------------------------------------------------------------
        # 1) ETF：全部放行
        # ------------------------------------------------------------
        cursor.execute(
            """
            UPDATE instruments
            SET is_tradable = TRUE
            WHERE asset_type = 'ETF'
            """
        )
        log.info(f"ETF marked tradable: {cursor.rowcount}")

        # ------------------------------------------------------------
        # 2) 状态异常 Stock -> FALSE
        # ------------------------------------------------------------
        cursor.execute(
            f"""
            UPDATE instruments i
            SET is_tradable = FALSE
            WHERE i.asset_type = 'Stock'
              AND i.status IN ('delisted','bankrupt','suspended')
              {scope_filter}
            """
        )
        log.info(f"Status filtered: {cursor.rowcount}")

        # ------------------------------------------------------------
        # 3) 仙股过滤（最新交易日价格）
        # ------------------------------------------------------------
        cursor.execute(
            f"""
            UPDATE instruments i
            SET is_tradable = FALSE
            FROM (
                SELECT DISTINCT instrument_id
                FROM market_prices
                WHERE date = %s
                  AND COALESCE(adj_close, close_price) < %s
            ) p
            WHERE i.instrument_id = p.instrument_id
              AND i.asset_type = 'Stock'
              {scope_filter}
            """,
            (last_px_date, min_price),
        )
        log.info(f"Low price filtered: {cursor.rowcount}")

        # ------------------------------------------------------------
        # 4) 低流动性过滤（窗口锚定 last_px_date）
        # ------------------------------------------------------------
        cursor.execute(
            f"""
            UPDATE instruments i
            SET is_tradable = FALSE
            FROM (
                SELECT instrument_id
                FROM market_prices
                WHERE date >= %s - INTERVAL '{volume_lookback_days} days'
                  AND date <= %s
                GROUP BY instrument_id
                HAVING AVG(COALESCE(volume, 0)) < %s
            ) v
            WHERE i.instrument_id = v.instrument_id
              AND i.asset_type = 'Stock'
              {scope_filter}
            """,
            (last_px_date, last_px_date, min_avg_volume),
        )
        log.info(f"Low volume filtered: {cursor.rowcount}")

        # ------------------------------------------------------------
        # 5) summary
        # ------------------------------------------------------------
        cursor.execute(
            """
            SELECT asset_type, COUNT(*)
            FROM instruments
            WHERE is_tradable = TRUE
            GROUP BY asset_type
            ORDER BY asset_type
            """
        )
        rows = cursor.fetchall()
        log.info("📊 Tradable universe summary:")
        for asset_type, cnt in rows:
            log.info(f"  {asset_type}: {cnt}")

        if commit:
            conn.commit()
            log.info("✅ Changes committed")
        else:
            conn.rollback()
            log.info("🟡 Dry run only, rolled back")

    except Exception as e:
        conn.rollback()
        log.exception(f"❌ Failed to update tradable universe: {e}")
        raise

    finally:
        conn.close()
