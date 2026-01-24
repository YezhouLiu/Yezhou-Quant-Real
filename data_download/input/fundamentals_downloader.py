import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import time
from typing import Optional, Dict

from database.utils.db_utils import get_db_connection
from database.readwrite.rw_instruments import get_all_instruments
from utils.logger import get_logger

from data_download.input.sec_edgar_fundamental_single import (
    download_one_ticker_fundamental_data,
)

log = get_logger("fundamentals_downloader")


# -----------------------------------------------------------------------------
# Main orchestrator
# -----------------------------------------------------------------------------
def download_fundamentals(
    *,
    tradable_only: bool = True,
    sleep_seconds: float = 0.25,
    exchange: Optional[str] = None,
) -> Dict[str, int]:
    """
    批量下载 instruments 中的基本面数据（SEC EDGAR）。

    规则：
    - 跳过 asset_type == 'ETF'
    - tradable_only=True 时，只下载 is_tradable=True
    - 每个 ticker 调用后 sleep，避免 SEC 临时封禁
    - 不写 system_state
    - 不吞异常：单 ticker 失败只计数，不影响其他

    返回统计信息
    """

    log.info("=" * 70)
    log.info("🚀 SEC EDGAR fundamentals downloader")
    log.info("=" * 70)
    log.info(f"tradable_only = {tradable_only}")
    log.info(f"sleep_seconds = {sleep_seconds}")
    log.info(f"exchange filter = {exchange or 'ALL'}")

    conn = get_db_connection()
    if not conn:
        log.error("❌ 无法创建数据库连接")
        return {"total": 0, "success": 0, "failed": 0, "skipped": 0}

    total = success = failed = skipped = 0

    try:
        instruments = get_all_instruments(conn, asset_type=None)

        if tradable_only:
            instruments = instruments[instruments["is_tradable"] == True]

        if exchange:
            instruments = instruments[instruments["exchange"] == exchange]

        if instruments.empty:
            log.warning("⚠️ 没有符合条件的 instruments")
            return {"total": 0, "success": 0, "failed": 0, "skipped": 0}

        total = len(instruments)
        log.info(f"📊 待处理 instruments: {total}")

        for idx, row in instruments.iterrows():
            ticker = row["ticker"]
            asset_type = row["asset_type"]
            exch = row["exchange"]

            # ETF 明确跳过
            if asset_type == "ETF":
                skipped += 1
                continue

            if (success + failed + skipped) % 20 == 0:
                log.info(
                    f"[{success + failed + skipped}/{total}] "
                    f"✅{success} ❌{failed} ⏭️{skipped}"
                )

            try:
                rows = download_one_ticker_fundamental_data(
                    conn,
                    ticker=ticker,
                    exchange=exch,
                )
                conn.commit()
                success += 1
                log.info(f"✅ {ticker}: wrote {rows} rows")

            except Exception as e:
                conn.rollback()
                failed += 1
                log.error(f"❌ {ticker}: {e}")

            # 🔒 SEC 节流（非常重要）
            time.sleep(sleep_seconds)

    finally:
        conn.close()

    log.info("=" * 70)
    log.info("✅ fundamentals download finished")
    log.info("=" * 70)
    log.info(f"Total   : {total}")
    log.info(f"Success : {success}")
    log.info(f"Failed  : {failed}")
    log.info(f"Skipped : {skipped}")
    log.info("=" * 70)

    return {
        "total": total,
        "success": success,
        "failed": failed,
        "skipped": skipped,
    }
