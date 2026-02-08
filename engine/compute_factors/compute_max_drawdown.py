from __future__ import annotations

from database.utils.db_utils import get_db_connection
from database.readwrite.rw_instruments import get_tradable_instrument_ids
from database.readwrite.rw_system_state import get_state, set_state
from database.readwrite.rw_market_prices import get_price_max_date
from factors.max_drawdown import calc_single_instrument_max_drawdown
from utils.config_values import DEFAULT_START_DATE
from utils.time import to_date
from utils.logger import get_logger

log = get_logger("compute_max_drawdown")


MDD_SPECS = [
    ("mdd_252d", 252),
]


def run(*, force: bool = False):
    conn = get_db_connection()
    if not conn:
        raise RuntimeError("failed to get db connection")

    try:
        instrument_ids = get_tradable_instrument_ids(conn)
        if not instrument_ids:
            log.warning("no tradable instruments found")
            return

        req_start = to_date(DEFAULT_START_DATE())

        max_db_date = get_price_max_date(conn)
        if not max_db_date:
            log.warning("market_prices is empty, nothing to do")
            return
        req_end = to_date(max_db_date)

        log.info(f"[range] requested range: {req_start} -> {req_end}")

        for tag, window in MDD_SPECS:
            log.info(f"== {tag}: window={window} ==")

            state_key = f"factor:max_drawdown:{window}:v1"
            st = get_state(conn, state_key, default=None)

            actual_start = req_start
            old_last_done = None
            if st and "last_done_date" in st:
                old_last_done = to_date(st["last_done_date"])

            if (not force) and old_last_done:
                if old_last_done > actual_start:
                    actual_start = old_last_done

            if actual_start > req_end:
                log.info("[mdd] already up to date, skip")
                continue

            total_written = 0
            failed = 0
            zero_written = 0

            for instrument_id in instrument_ids:
                try:
                    n = calc_single_instrument_max_drawdown(
                        conn,
                        instrument_id=instrument_id,
                        start_date=actual_start,
                        end_date=req_end,
                        window=window,
                        factor_version="v1",
                    )
                    total_written += n
                    if n == 0:
                        zero_written += 1
                except Exception as e:
                    failed += 1
                    log.warning(f"[mdd] instrument {instrument_id} failed: {e}")

            log.info(
                f"[mdd] {tag} finished: wrote={total_written}, "
                f"zero_written_instruments={zero_written}, failed_instruments={failed}"
            )

            if total_written > 0:
                new_last_done = req_end
                if old_last_done and old_last_done > new_last_done:
                    new_last_done = old_last_done

                set_state(
                    conn,
                    state_key,
                    {
                        "last_done_date": new_last_done.isoformat(),
                        "window": window,
                        "factor": "max_drawdown",
                        "version": "v1",
                    },
                )
                conn.commit()
            else:
                log.warning(f"[state] {tag}: wrote 0 rows, state not advanced")

    finally:
        conn.close()
