# =============================================================================
# Yezhou Capital Limited  |  Proprietary & Confidential
# =============================================================================
# Copyright (c) 2026 Yezhou Capital Limited. All rights reserved.
#
# Project  : Yezhou Quantitative Trading System
# Author   : Yezhou Liu
# Contact  : yezhoucapital@gmail.com
#
# This source code is the exclusive property of Yezhou Capital Limited.
# Unauthorized copying, modification, distribution, or use of this file,
# via any medium, is strictly prohibited without prior written consent.
# =============================================================================
from __future__ import annotations

from database.utils.db_utils import get_db_connection
from database.readwrite.rw_instruments import get_tradable_instrument_ids
from database.readwrite.rw_system_state import get_state, set_state
from database.readwrite.rw_market_prices import get_price_max_date
from factors.decline_streak import calc_single_instrument_decline_streak
from utils.config_values import DEFAULT_START_DATE
from utils.time import to_date
from utils.logger import get_logger

log = get_logger("compute_decline_streak")


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
        log.info("== decline_streak ==")

        state_key = "factor:decline_streak:v1"
        st = get_state(conn, state_key, default=None)

        actual_start = req_start
        old_last_done = None
        if st and "last_done_date" in st:
            old_last_done = to_date(st["last_done_date"])

        if (not force) and old_last_done:
            if old_last_done > actual_start:
                actual_start = old_last_done

        if actual_start > req_end:
            log.info("[decline_streak] already up to date, skip")
            return

        total_written = 0
        failed = 0
        zero_written = 0

        for instrument_id in instrument_ids:
            try:
                n = calc_single_instrument_decline_streak(
                    conn,
                    instrument_id=instrument_id,
                    start_date=actual_start,
                    end_date=req_end,
                    factor_version="v1",
                )
                total_written += n
                if n == 0:
                    zero_written += 1
            except Exception as e:
                failed += 1
                log.warning(f"[decline_streak] instrument {instrument_id} failed: {e}")

        log.info(
            f"[decline_streak] finished: wrote={total_written}, "
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
                    "factor": "decline_streak",
                    "version": "v1",
                },
            )
            conn.commit()
        else:
            log.warning("[state] decline_streak: wrote 0 rows, state not advanced")

    finally:
        conn.close()
