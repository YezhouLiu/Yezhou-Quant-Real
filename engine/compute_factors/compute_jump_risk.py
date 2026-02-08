from __future__ import annotations

from database.utils.db_utils import get_db_connection
from database.readwrite.rw_instruments import get_tradable_instrument_ids
from database.readwrite.rw_system_state import get_state, set_state
from database.readwrite.rw_market_prices import get_price_max_date
from factors.jump_risk import calc_single_instrument_jump_risk
from utils.config_values import DEFAULT_START_DATE
from utils.time import to_date
from utils.logger import get_logger

log = get_logger("compute_jump_risk")


JUMP_SPECS = [
    ("jump_60d", 60),
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

        for tag, window in JUMP_SPECS:
            state_key = f"factor:jump:{window}:v1"
            st = get_state(conn, state_key, default=None)

            actual_start = req_start
            old_last_done = None
            if st and "last_done_date" in st:
                old_last_done = to_date(st["last_done_date"])

            if (not force) and old_last_done:
                if old_last_done > actual_start:
                    actual_start = old_last_done

            if actual_start > req_end:
                log.info("[jump] already up to date, skip")
                continue

            total_written = 0

            for instrument_id in instrument_ids:
                n = calc_single_instrument_jump_risk(
                    conn,
                    instrument_id=instrument_id,
                    start_date=actual_start,
                    end_date=req_end,
                    window=window,
                    factor_version="v1",
                )
                total_written += n

            if total_written > 0:
                set_state(
                    conn,
                    state_key,
                    {
                        "last_done_date": req_end.isoformat(),
                        "window": window,
                        "factor": "jump_risk",
                        "version": "v1",
                    },
                )
                conn.commit()

    finally:
        conn.close()
