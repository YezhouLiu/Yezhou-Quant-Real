"""
公司行为抽取器（DB -> DB）
=========================

从 market_prices 抽取公司行为（分红/拆股）写入 corporate_actions。

抽取规则：
- dividends != 0 -> DIVIDEND_CASH (action_value = dividends)
- stock_splits != 1 -> SPLIT / REVERSE_SPLIT
  - stock_splits > 1 -> SPLIT
  - stock_splits < 1 -> REVERSE_SPLIT
  - action_value = stock_splits

状态推进（非常关键）：
- state 记录“已处理到 market_prices 的最后一日”
- 推进时永远以 DB 的 MAX(market_prices.date) 为准
  => 不会被用户传 end_date=2090 这种污染
"""

import sys
from pathlib import Path
from database.readwrite.rw_market_prices import get_price_max_date, get_price_min_date
from utils.time import to_date

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from datetime import date, timedelta
from typing import Optional, Dict, Any, List, Tuple

from database.utils.db_utils import get_db_connection
from database.readwrite.rw_system_state import get_state, set_state
from database.readwrite.rw_corporate_actions import batch_insert_corporate_actions
from utils.logger import get_logger

log = get_logger("corporate_actions_extractor")


def _resolve_date_range(
    conn,
    start_date: Optional[date],
    end_date: Optional[date],
    state_key: str,
) -> Tuple[date, date]:
    if end_date is None:
        end_date = get_price_max_date(conn)
        if end_date is None:
            return date.max, date.min  # 触发 start>end 秒退

    if start_date is not None:
        return to_date(start_date), to_date(end_date)

    last_done = get_state(conn, state_key)
    if last_done:
        start = to_date(last_done) + timedelta(days=1)
        log.info(f"📅 继续抽取公司行为: {start}")
        return to_date(start), to_date(end_date)

    min_dt = get_price_min_date(conn)
    if min_dt is None:
        return date.max, date.min

    log.info(f"📅 首次抽取公司行为: {min_dt}")
    return to_date(min_dt), to_date(end_date)


def _extract_actions_from_market_prices(
    conn,
    start_date: date,
    end_date: date,
) -> List[Dict[str, Any]]:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT instrument_id, date, dividends, stock_splits
        FROM market_prices
        WHERE date >= %s AND date <= %s
          AND (
              (dividends IS NOT NULL AND dividends <> 0)
           OR (stock_splits IS NOT NULL AND stock_splits <> 1)
          )
        ORDER BY date, instrument_id
        """,
        (start_date.isoformat(), end_date.isoformat()),
    )

    rows = cursor.fetchall()
    actions: List[Dict[str, Any]] = []

    for instrument_id, dt, dividends, stock_splits in rows:
        action_date = dt.isoformat() if hasattr(dt, "isoformat") else str(dt)

        if dividends is not None and dividends != 0:
            actions.append(
                {
                    "instrument_id": instrument_id,
                    "action_date": action_date,
                    "action_type": "DIVIDEND_CASH",
                    "action_value": float(dividends),
                    "currency": "USD",
                    "data_source": "tiingo",
                    "raw_payload": None,
                }
            )

        if stock_splits is not None and stock_splits != 1:
            sf = float(stock_splits)
            action_type = "SPLIT" if sf > 1 else "REVERSE_SPLIT"
            actions.append(
                {
                    "instrument_id": instrument_id,
                    "action_date": action_date,
                    "action_type": action_type,
                    "action_value": sf,
                    "currency": "USD",
                    "data_source": "tiingo",
                    "raw_payload": None,
                }
            )

    return actions


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def extract_corporate_actions(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    batch_size: int = 50000,
    state_key: str = "last_corporate_actions_extract",
) -> Dict[str, int]:
    """
    从 market_prices 抽取公司行为写入 corporate_actions，并推进 state。
    """
    log.info("=" * 70)
    log.info("🧾 公司行为抽取（market_prices -> corporate_actions）")
    log.info("=" * 70)

    conn = get_db_connection()
    if not conn:
        log.error("❌ 无法创建数据库连接")
        return {"inserted": 0, "events": 0}

    inserted = 0
    events = 0

    try:
        start_date, end_date = _resolve_date_range(
            conn, start_date, end_date, state_key
        )

        if start_date > end_date:
            log.info(f"🟢 无需抽取（start_date={start_date} > end_date={end_date}）")
            return {"inserted": 0, "events": 0}

        log.info(f"📅 抽取区间: {start_date} → {end_date}")

        actions = _extract_actions_from_market_prices(conn, start_date, end_date)
        events = len(actions)

        if not actions:
            log.info("🟢 本区间无公司行为事件")
        else:
            for i in range(0, len(actions), batch_size):
                chunk = actions[i : i + batch_size]
                batch_insert_corporate_actions(conn, chunk)
                conn.commit()
                inserted += len(chunk)

            log.info(f"✅ 写入公司行为: {inserted} 条（含 upsert）")

        # 推进 state：永远以 market_prices MAX(date) 为准
        max_dt = get_price_max_date(conn)
        if max_dt is not None:
            max_dt = to_date(max_dt)
            set_state(conn, state_key, max_dt.isoformat())
            conn.commit()
            log.info(f"✅ 更新抽取位置(按 market_prices 已落库最后一日): {max_dt}")

    finally:
        conn.close()

    return {"inserted": inserted, "events": events}
