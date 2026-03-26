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
"""
从 Tiingo 下载价格数据并写入数据库
核心特性：
1) 增量下载：从 system_state 读取上次下载位置，只下载缺失日期区间
2) 高效连接：单个 HTTP Session + 单个 DB Connection
3) 批量写入：减少 DB 交互次数
4) 状态追踪：仅在失败率足够低时推进 system_state，避免数据缺口
"""

import sys
from pathlib import Path
from database.readwrite.rw_trading_calendar import get_prev_trading_day
from utils.time import DATE_TODAY, to_date

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from datetime import date, timedelta
from typing import Optional, Dict, Tuple
import requests
from urllib3.util.retry import Retry
from typing import Optional, Dict, List, Any, Tuple
from database.readwrite.rw_instruments import get_all_instruments
from database.readwrite.rw_market_prices import batch_insert_prices, get_price_max_date
from database.readwrite.rw_system_state import get_state, set_state
from database.utils.db_utils import get_db_connection
from utils.config_loader import get_config_value
from utils.config_values import DEFAULT_START_DATE
from utils.logger import get_logger

log = get_logger("price_downloader")


# -----------------------------------------------------------------------------
# HTTP Session / Retry
# -----------------------------------------------------------------------------
def _build_session() -> requests.Session:
    """
    构建带 status-aware retry 的 Session。
    说明：
    - 重点覆盖 429（rate limit）与常见 5xx。
    - backoff_factor=0.5 -> 0.5s, 1s, 2s...（由 urllib3 计算）
    """
    session = requests.Session()

    retry = Retry(
        total=3,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        backoff_factor=0.5,
        raise_on_status=False,
        respect_retry_after_header=True,  # 如果服务端返回 Retry-After，会更聪明
    )

    adapter = requests.adapters.HTTPAdapter(
        max_retries=retry,
        pool_connections=1,
        pool_maxsize=1,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# -----------------------------------------------------------------------------
# State logic
# -----------------------------------------------------------------------------
def _resolve_date_range(
    conn, start_date: Optional[date], end_date: Optional[date]
) -> Tuple[date, date]:
    # ---------- end_date ----------
    if end_date is None:
        today: date = DATE_TODAY()  # 明确标注，Pylance 不会 Unknown
        prev_td: Optional[str] = get_prev_trading_day(
            conn, today.isoformat(), market="US"
        )
        if prev_td is None:
            raise RuntimeError(
                "trading_calendar missing or no previous trading day found"
            )
        end_date = to_date(prev_td)
    else:
        end_date = to_date(end_date)

    # ---------- start_date ----------
    if start_date is not None:
        return to_date(start_date), end_date

    last_db_date = get_price_max_date(conn) # market_prices 最大 date
    if last_db_date:
        start = to_date(last_db_date) + timedelta(days=1)
        log.info(f"📅 继续增量下载（基于 market_prices.max(date)）: {start}")
        return start, end_date

    start = to_date(DEFAULT_START_DATE())
    log.info(f"📅 首次下载: {start}")
    return start, end_date


def _should_advance_state(
    *,
    requested: int,
    success: int,
    failed: int,
    max_failure_rate: float = 0.01,
    min_success_to_advance: int = 1,
) -> bool:
    """
    是否推进 last_price_download。

    核心思想：
    - 不让少量抖动阻止推进（否则永远重跑浪费）
    - 也不让失败过多造成缺口（否则增量会“跳过坑”）

    规则：
    - 至少有 min_success_to_advance 成功
    - failed/requested < max_failure_rate
    - 允许一定数量的绝对失败：max(20, 0.5% * requested)
      （这样小 universe 不苛刻，大 universe 不被绝对数卡死）
    """
    if success < min_success_to_advance:
        return False
    if requested <= 0:
        return False

    failure_rate = failed / requested
    max_failed_abs = max(20, int(0.005 * requested))  # 0.5% or 20 whichever larger

    return (failure_rate < max_failure_rate) and (failed <= max_failed_abs)


# -----------------------------------------------------------------------------
# Main downloader
# -----------------------------------------------------------------------------
def download_prices(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    asset_types: Optional[list] = None,
    batch_size: int = 500,
) -> Dict[str, int]:
    log.info("=" * 70)
    log.info("🚀 价格数据下载")
    log.info("=" * 70)

    api_token = get_config_value("tiingo.api_key")
    if not api_token:
        log.error("❌ 未配置 Tiingo API Token")
        return {"success": 0, "failed": 0, "skipped": 0, "total": 0, "records": 0}

    conn = get_db_connection()
    if not conn:
        log.error("❌ 无法创建数据库连接")
        return {"success": 0, "failed": 0, "skipped": 0, "total": 0, "records": 0}

    # 统计（requested 表示真正发起的网络请求次数）
    total = 0
    requested = 0
    success = 0
    failed = 0
    skipped = 0
    total_records = 0
    pending_batch = []

    try:
        start_date, end_date = _resolve_date_range(conn, start_date, end_date)

        if start_date > end_date:
            log.info(
                f"🟢 价格数据已是最新，无需下载 "
                f"(start_date={start_date} > end_date={end_date})"
            )
            conn.close()
            return {
                "success": 0,
                "failed": 0,
                "skipped": 0,
                "total": 0,
                "records": 0,
                "requested": 0,
            }

        log.info(f"📅 日期范围: {start_date} → {end_date}")
        log.info(f"📦 批量大小: {batch_size}条")

        instruments_df = get_all_instruments(conn, asset_type=None)
        if asset_types:
            instruments_df = instruments_df[
                instruments_df["asset_type"].isin(asset_types)
            ]

        total = len(instruments_df)
        if total == 0:
            log.warning("⚠️  没有找到需要下载的instruments")
            return {"success": 0, "failed": 0, "skipped": 0, "total": 0, "records": 0}

        log.info(f"📊 待下载: {total} 个instruments\n")

        session = _build_session()
        try:
            for idx, row in instruments_df.iterrows():
                ticker = row["ticker"]
                instrument_id = row["instrument_id"]

                if (idx + 1) % 50 == 0 or idx == 0:
                    pct = (idx + 1) / total * 100
                    log.info(
                        f"[{idx+1}/{total}] {pct:.1f}% | ✅{success} ⏭️{skipped} ❌{failed} | 🌐{requested}req | 📊{total_records}条"
                    )

                requested += 1

                try:
                    tiingo_data = fetch_tiingo_prices(
                        ticker, start_date, end_date, api_token, session
                    )
                except Exception as e:
                    # fetch 内部可能抛异常（网络/解析等），这里捕获并计 failed，继续下一只
                    failed += 1
                    log.error(f"❌ {ticker}: fetch failed: {e}")
                    continue

                # 语义约定：None = 请求失败；[] = 合法但无数据
                if tiingo_data is None:
                    failed += 1
                    log.error(f"❌ {ticker}: request failed (None)")
                    continue

                if len(tiingo_data) == 0:
                    skipped += 1
                    continue

                try:
                    db_records = transform_tiingo_price_data_to_db_format(
                        tiingo_data, instrument_id
                    )
                except Exception as e:
                    failed += 1
                    log.error(f"❌ {ticker}: transform failed: {e}")
                    continue

                if not db_records:
                    skipped += 1
                    continue

                pending_batch.extend(db_records)
                success += 1

                if len(pending_batch) >= batch_size:
                    insert_count = len(pending_batch)
                    try:
                        batch_insert_prices(conn, pending_batch)
                        conn.commit()
                        total_records += insert_count
                        pending_batch = []
                    except Exception as e:
                        conn.rollback()
                        # DB 写入失败属于严重问题：计 failed，并继续（避免全盘崩）
                        failed += 1
                        log.error(f"❌ DB insert failed (batch {insert_count}): {e}")
                        pending_batch = []

            # flush remaining
            if pending_batch:
                insert_count = len(pending_batch)
                try:
                    batch_insert_prices(conn, pending_batch)
                    conn.commit()
                    total_records += insert_count
                except Exception as e:
                    conn.rollback()
                    failed += 1
                    log.error(f"❌ DB insert failed (final batch {insert_count}): {e}")

        finally:
            session.close()

        # 推进 state（只在失败率足够低时）
        if _should_advance_state(requested=requested, success=success, failed=failed):
            last_date = get_price_max_date(conn)  # 获取 market_prices 目前最大 date
            if last_date is not None:
                last_date = to_date(last_date)
                set_state(conn, "last_price_download", last_date.isoformat())
                conn.commit()
                log.info(f"\n✅ 更新下载位置(已落库最后一日): {last_date}")
            else:
                log.warning("\n⚠️ 未更新下载位置：market_prices 为空")
        else:
            failure_rate = (failed / requested) if requested else 0.0
            log.warning(
                f"\n⚠️ 未更新下载位置：失败率/失败数不达标 (失败率: {failure_rate*100:.2f}%, 失败数: {failed}, 请求数: {requested}), 避免数据缺口"
            )

    finally:
        conn.close()

    # 汇总
    log.info("\n" + "=" * 70)
    log.info("✅ 下载完成")
    log.info("=" * 70)
    log.info(f"总计 instruments: {total}")
    if total > 0:
        log.info(f"✅ 成功: {success} ({success/total*100:.1f}%)")
        log.info(f"⏭️  无数据: {skipped} ({skipped/total*100:.1f}%)")
        log.info(f"❌ 失败: {failed} ({failed/total*100:.1f}%)")
    log.info(f"🌐 请求数: {requested}")
    log.info(f"📊 插入: {total_records} 条记录")
    log.info("=" * 70 + "\n")

    return {
        "success": success,
        "failed": failed,
        "skipped": skipped,
        "total": total,
        "records": total_records,
        "requested": requested,
    }


def fetch_tiingo_prices(
    ticker: str,
    start_date: date,
    end_date: date,
    api_token: str,
    session: requests.Session,
) -> Optional[List[Dict[str, Any]]]:
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
    headers = {"Authorization": f"Token {api_token}"}
    params = {
        "startDate": start_date.strftime("%Y-%m-%d"),
        "endDate": end_date.strftime("%Y-%m-%d"),
        "format": "json",
    }

    try:
        resp = session.get(url, headers=headers, params=params, timeout=15)
    except Exception as e:
        log.error(f"❌ {ticker}: request exception: {e}")
        return None

    if resp.status_code != 200:
        snippet = (resp.text or "")[:200].replace("\n", " ")
        log.warning(f"⚠️ {ticker}: HTTP {resp.status_code} | {snippet}")
        return None

    # 200：解析 JSON
    try:
        data = resp.json()
    except Exception as e:
        snippet = (resp.text or "")[:200].replace("\n", " ")
        log.error(f"❌ {ticker}: JSON decode failed: {e} | {snippet}")
        return None

    # Tiingo 这里应当返回 list
    if not isinstance(data, list):
        # 不能当 []，否则把错误伪装成“无数据”
        snippet = str(data)[:200].replace("\n", " ")
        log.error(f"❌ {ticker}: unexpected JSON type {type(data)} | {snippet}")
        return None

    # list：可能为空
    return data


def transform_tiingo_price_data_to_db_format(
    tiingo_data: List[Dict[str, Any]],
    instrument_id: int,
) -> List[Dict[str, Any]]:
    """
    转换 Tiingo 响应为数据库格式。
    注意：这里默认不吞异常（早暴露问题）。
    """
    db_records: List[Dict[str, Any]] = []

    for record in tiingo_data:
        date_str = record["date"][:10]

        db_records.append(
            {
                "instrument_id": instrument_id,
                "date": date_str,
                "open_price": record.get("open"),
                "high_price": record.get("high"),
                "low_price": record.get("low"),
                "close_price": record.get("close"),
                "volume": record.get("volume"),
                "adj_open": record.get("adjOpen"),
                "adj_high": record.get("adjHigh"),
                "adj_low": record.get("adjLow"),
                "adj_close": record.get("adjClose"),
                "adj_volume": record.get("adjVolume"),
                "dividends": record.get("divCash", 0),
                "stock_splits": record.get("splitFactor", 1),
                "data_source": "tiingo",
            }
        )

    return db_records
