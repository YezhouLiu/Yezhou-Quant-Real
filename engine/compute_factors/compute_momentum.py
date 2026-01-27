from __future__ import annotations

from database.utils.db_utils import get_db_connection
from database.readwrite.rw_instruments import get_tradable_instrument_ids
from database.readwrite.rw_system_state import get_state, set_state
from database.readwrite.rw_market_prices import get_price_max_date
from factors.momentum import calc_single_instrument_momentum
from utils.config_values import DEFAULT_START_DATE
from utils.time import to_date
from utils.logger import get_logger

log = get_logger("compute_momentum")


MOMENTUM_SPECS = [
    ("mom_12m_skip1m", 252, 21),
    ("mom_6m", 126, 0),
    ("mom_3m", 63, 0),
    ("mom_1m", 21, 0),
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

        log.info(
            f"[universe] tradable instruments: n={len(instrument_ids)}, sample={instrument_ids[:5]}"
        )

        req_start = to_date(DEFAULT_START_DATE())

        max_db_date = get_price_max_date(conn)
        if not max_db_date:
            log.warning("market_prices is empty, nothing to do")
            return
        req_end = to_date(max_db_date)

        log.info(f"[range] requested range: {req_start} -> {req_end}")

        for tag, lookback, skip in MOMENTUM_SPECS:
            log.info(f"== {tag}: lookback={lookback}, skip={skip} ==")

            state_key = f"factor:momentum:{lookback}:{skip}:v1"
            st = get_state(conn, state_key, default=None)

            actual_start = req_start
            old_last_done = None

            if st and "last_done_date" in st:
                old_last_done = to_date(st["last_done_date"])

            # force=False 时，从 old_last_done 开始续算（不 +1，rolling 因子安全）
            if (not force) and old_last_done:
                if old_last_done > actual_start:
                    actual_start = old_last_done

            log.info(
                f"[state] actual_start={actual_start}, old_last_done={old_last_done}, force={force}"
            )

            if actual_start > req_end:
                log.info("[momentum] already up to date, skip")
                continue

            total_written = 0
            failed = 0
            zero_written = 0

            for idx, instrument_id in enumerate(instrument_ids):
                try:
                    n = calc_single_instrument_momentum(
                        conn,
                        instrument_id=instrument_id,
                        start_date=actual_start,
                        end_date=req_end,
                        lookback=lookback,
                        skip=skip,
                        factor_version="v1",
                    )
                    total_written += n
                    if n == 0:
                        zero_written += 1

                    if idx < 3:
                        log.info(
                            f"[debug] instrument={instrument_id}, written_rows={n}"
                        )

                except Exception as e:
                    failed += 1
                    log.warning(f"[momentum] instrument {instrument_id} failed: {e}")

            log.info(
                f"[momentum] {tag} finished: wrote={total_written}, "
                f"zero_written_instruments={zero_written}, failed_instruments={failed}"
            )

            # 只有真的写出数据才推进 state，避免“跑空也推进”掩盖问题
            if total_written > 0:
                new_last_done = req_end
                if old_last_done and old_last_done > new_last_done:
                    new_last_done = old_last_done

                set_state(
                    conn,
                    state_key,
                    {
                        "last_done_date": new_last_done.isoformat(),
                        "lookback": lookback,
                        "skip": skip,
                        "factor": "momentum",
                        "version": "v1",
                    },
                )
                conn.commit()
            else:
                log.warning(f"[state] {tag}: wrote 0 rows, state not advanced")

    finally:
        conn.close()
