import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from datetime import date, timedelta
import pandas as pd
import pandas_market_calendars as mcal

from utils.logger import get_logger
from utils.time import latest_us_market_date, to_date
from database.utils.db_utils import get_db_connection
from database.readwrite.rw_trading_calendar import batch_insert_trading_days

log = get_logger("trading_calendar_builder")


# ---------------------------------------------------------------------
# internal helpers
# ---------------------------------------------------------------------
def _get_existing_max_date(conn, market: str) -> date | None:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT MAX(date)
        FROM trading_calendar
        WHERE market = %s
        """,
        (market,),
    )
    row = cursor.fetchone()
    return row[0] if row and row[0] else None


# ---------------------------------------------------------------------
# public API
# ---------------------------------------------------------------------
def build_trading_calendar(
    *,
    market: str = "US",
    exchange: str = "NYSE",
    start_date: date | None = None,
    end_date: date | None = None,
    horizon_days: int = 365,
    commit: bool = True,
):
    """
    构建 / 增量更新 trading_calendar

    规则（务实版）：
    - end_date = latest_us_market_date() + horizon_days
    - start_date：
        - 若传入，直接使用
        - 否则从 trading_calendar.max(date) + 1
        - 若表为空，从 2000-01-01
    """
    conn = get_db_connection()
    if not conn:
        raise RuntimeError("❌ DB connection failed")

    try:
        # ---------- resolve end_date ----------
        if end_date is None:
            end_date = latest_us_market_date() + timedelta(days=horizon_days)
        end_date = to_date(end_date)

        # ---------- resolve start_date ----------
        if start_date is None:
            max_dt = _get_existing_max_date(conn, market)
            if max_dt is None:
                start_date = date(2000, 1, 1)
                log.info(f"📅 trading_calendar empty → start from {start_date}")
            else:
                start_date = max_dt + timedelta(days=1)
                log.info(f"📅 trading_calendar max={max_dt} → incremental start {start_date}")
        start_date = to_date(start_date)

        if start_date > end_date:
            log.info("🟢 trading_calendar already up-to-date")
            return

        log.info("=" * 70)
        log.info("📅 Build trading calendar")
        log.info(f"market     = {market}")
        log.info(f"exchange   = {exchange}")
        log.info(f"start_date = {start_date}")
        log.info(f"end_date   = {end_date}")
        log.info(f"commit     = {commit}")
        log.info("=" * 70)

        # ---------- generate trading days ----------
        cal = mcal.get_calendar(exchange)
        schedule = cal.schedule(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )
        trading_days = set(schedule.index.date)

        # ---------- generate full day range ----------
        all_days = pd.date_range(start_date, end_date, freq="D")

        rows = []
        for d in all_days:
            dd = d.date()
            rows.append(
                {
                    "date": dd,
                    "is_trading_day": dd in trading_days,
                    "holiday_name": None,
                }
            )

        batch_insert_trading_days(conn, rows, market=market)

        if commit:
            conn.commit()
            log.info(f"✅ trading_calendar updated: {len(rows)} days")
        else:
            conn.rollback()
            log.info(f"🟡 dry-run rollback (prepared {len(rows)} days)")

    finally:
        conn.close()


if __name__ == "__main__":
    build_trading_calendar()
