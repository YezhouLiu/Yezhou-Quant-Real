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
每日市场情报简报

从 factor_values 和 market_prices 中查询最新数据，生成分类榜单：
  - MOMENTUM LEADERS / LAGGARDS   谁最近涨/跌得最凶
  - VOLUME SPIKES                 谁的交易量异常放大（事件驱动信号）
  - DECLINE STREAKS               谁连续多天在下跌
  - VOLATILITY ALERTS             谁的波动率突然放大
  - SECTOR SUMMARY                板块级别涨跌汇总
"""
from __future__ import annotations

from typing import Optional

import pandas as pd

import os

from database.utils.db_utils import get_db_connection
from database.readwrite.rw_factor_values import get_factor_values
from utils.config_loader import get_config_value
from utils.logger import get_logger

log = get_logger("daily_briefing")

# Factor names as stored in factor_values
_FACTOR_NAMES = [
    "mom_1d",
    "mom_5d",
    "mom_21d",
    "mom_63d",
    "vol_20d_ann252",
    "mdd_252d",
    "dv_20d_log",
    "vol_ratio_20d",
    "decline_streak",
]

# Thresholds for alert sections (tunable)
_VOL_SPIKE_MIN_RATIO = 2.0     # vol_ratio >= 2x to appear in spikes section
_DECLINE_STREAK_MIN_DAYS = 3   # >= 3 consecutive down days
_VOL_ALERT_MIN = 0.50          # annualised vol >= 50%


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_factor_snapshot(conn, date: str) -> pd.DataFrame:
    """
    Returns a wide DataFrame with one row per tradable instrument on `date`.
    Columns: instrument_id, ticker, company_name, sector, <factor_name>...
    """
    cursor = conn.cursor()

    # Single SQL join: factor_values × instruments for this date
    cursor.execute(
        """
        SELECT
            i.instrument_id,
            i.ticker,
            i.company_name,
            i.sector,
            fv.factor_name,
            fv.factor_value
        FROM factor_values fv
        JOIN instruments i ON i.instrument_id = fv.instrument_id
        WHERE fv.date = %s
          AND fv.factor_version = 'v1'
          AND i.is_tradable = TRUE
          AND fv.factor_name = ANY(%s)
        ORDER BY i.ticker, fv.factor_name
        """,
        (date, _FACTOR_NAMES),
    )
    rows = cursor.fetchall()

    if not rows:
        return pd.DataFrame()

    raw = pd.DataFrame(rows, columns=["instrument_id", "ticker", "company_name", "sector", "factor_name", "factor_value"])

    # Pivot: one row per instrument, one column per factor
    wide = raw.pivot_table(
        index=["instrument_id", "ticker", "company_name", "sector"],
        columns="factor_name",
        values="factor_value",
        aggfunc="first",
    ).reset_index()
    wide.columns.name = None

    # Ensure all expected factor columns exist (fill missing with NaN)
    for fname in _FACTOR_NAMES:
        if fname not in wide.columns:
            wide[fname] = float("nan")
        else:
            wide[fname] = pd.to_numeric(wide[fname], errors="coerce")

    return wide


def _load_price_snapshot(conn, date: str) -> pd.DataFrame:
    """Returns today's raw OHLCV for all tradable instruments."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            mp.instrument_id,
            mp.adj_close,
            mp.adj_volume,
            mp.high_price,
            mp.low_price,
            mp.open_price
        FROM market_prices mp
        JOIN instruments i ON i.instrument_id = mp.instrument_id
        WHERE mp.date = %s
          AND i.is_tradable = TRUE
        """,
        (date,),
    )
    rows = cursor.fetchall()
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        rows,
        columns=["instrument_id", "adj_close", "adj_volume", "high_price", "low_price", "open_price"],
    )
    for col in ["adj_close", "adj_volume", "high_price", "low_price", "open_price"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Intraday range % = (high - low) / open
    df["intraday_range_pct"] = (df["high_price"] - df["low_price"]) / df["open_price"].replace(0, float("nan"))
    return df


def _get_latest_factor_date(conn) -> Optional[str]:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT MAX(date) FROM factor_values WHERE factor_version = 'v1'"
    )
    row = cursor.fetchone()
    if not row or row[0] is None:
        return None
    d = row[0]
    return d.isoformat() if hasattr(d, "isoformat") else str(d)


def _fmt_pct(v, decimals: int = 1) -> str:
    if pd.isna(v):
        return "  n/a"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v * 100:.{decimals}f}%"


def _fmt_ratio(v, decimals: int = 1) -> str:
    if pd.isna(v):
        return "  n/a"
    return f"{v:.{decimals}f}x"


def _fmt_streak(v) -> str:
    if pd.isna(v):
        return "n/a"
    return f"{int(v)}d"


def _section_header(title: str, width: int = 70) -> str:
    line = "-" * width
    return f"\n{title}\n{line}"


def _table_row(rank: int, ticker: str, name: str, sector: str, *extras) -> str:
    name_trunc = (name or "")[:24].ljust(24)
    sector_trunc = (sector or "")[:16].ljust(16)
    extra_str = "  ".join(str(e) for e in extras)
    return f"  {rank:>3}.  {ticker:<8}  {name_trunc}  {sector_trunc}  {extra_str}"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_briefing(
    conn=None,
    date: Optional[str] = None,
    top_n: int = 20,
) -> dict:
    """
    Build a structured daily briefing dictionary.

    Parameters
    ----------
    conn : psycopg connection (optional; opens one if None)
    date : ISO date string (optional; uses latest available factor date)
    top_n : max rows per section

    Returns
    -------
    dict with keys: date, sections (list of section dicts), raw_df
    Each section: {name, label, rows (list of dicts)}
    """
    _owns_conn = conn is None
    if _owns_conn:
        conn = get_db_connection()
        if not conn:
            log.error("failed to get db connection")
            return {"date": None, "sections": [], "raw_df": pd.DataFrame()}

    try:
        if date is None:
            date = _get_latest_factor_date(conn)
        if date is None:
            log.warning("no factor data found in DB")
            return {"date": None, "sections": [], "raw_df": pd.DataFrame()}

        log.info(f"[briefing] generating for date={date}")

        wide = _load_factor_snapshot(conn, date)
        prices = _load_price_snapshot(conn, date)

        if wide.empty:
            log.warning(f"[briefing] no factor data for date={date}")
            return {"date": date, "sections": [], "raw_df": pd.DataFrame()}

        # Merge intraday range into wide table
        if not prices.empty:
            wide = wide.merge(
                prices[["instrument_id", "adj_close", "adj_volume", "intraday_range_pct"]],
                on="instrument_id",
                how="left",
            )
        else:
            wide["adj_close"] = float("nan")
            wide["adj_volume"] = float("nan")
            wide["intraday_range_pct"] = float("nan")

        sections = []

        # ------------------------------------------------------------------
        # 1. MOMENTUM LEADERS — top_n by 5-day return
        # ------------------------------------------------------------------
        if "mom_5d" in wide.columns:
            sub = wide.dropna(subset=["mom_5d"]).nlargest(top_n, "mom_5d")
            rows = [
                {
                    "ticker": r.ticker,
                    "company_name": r.company_name,
                    "sector": r.sector,
                    "mom_1d": getattr(r, "mom_1d", float("nan")),
                    "mom_5d": r.mom_5d,
                    "mom_21d": getattr(r, "mom_21d", float("nan")),
                }
                for r in sub.itertuples(index=False)
            ]
            sections.append({"name": "momentum_leaders", "label": "MOMENTUM LEADERS (Top by 5d Return)", "rows": rows})

        # ------------------------------------------------------------------
        # 2. MOMENTUM LAGGARDS — bottom_n by 5-day return
        # ------------------------------------------------------------------
        if "mom_5d" in wide.columns:
            sub = wide.dropna(subset=["mom_5d"]).nsmallest(top_n, "mom_5d")
            rows = [
                {
                    "ticker": r.ticker,
                    "company_name": r.company_name,
                    "sector": r.sector,
                    "mom_1d": getattr(r, "mom_1d", float("nan")),
                    "mom_5d": r.mom_5d,
                    "mom_21d": getattr(r, "mom_21d", float("nan")),
                }
                for r in sub.itertuples(index=False)
            ]
            sections.append({"name": "momentum_laggards", "label": "MOMENTUM LAGGARDS (Bottom by 5d Return)", "rows": rows})

        # ------------------------------------------------------------------
        # 3. VOLUME SPIKES — today's volume >= 2x 20-day average
        # ------------------------------------------------------------------
        if "vol_ratio_20d" in wide.columns:
            spikes = wide[wide["vol_ratio_20d"] >= _VOL_SPIKE_MIN_RATIO].dropna(subset=["vol_ratio_20d"])
            sub = spikes.nlargest(top_n, "vol_ratio_20d")
            rows = [
                {
                    "ticker": r.ticker,
                    "company_name": r.company_name,
                    "sector": r.sector,
                    "vol_ratio_20d": r.vol_ratio_20d,
                    "mom_1d": getattr(r, "mom_1d", float("nan")),
                    "mom_5d": getattr(r, "mom_5d", float("nan")),
                }
                for r in sub.itertuples(index=False)
            ]
            sections.append({"name": "volume_spikes", "label": f"VOLUME SPIKES (Vol Ratio >= {_VOL_SPIKE_MIN_RATIO:.0f}x, Top by Ratio)", "rows": rows})

        # ------------------------------------------------------------------
        # 4. DECLINE STREAKS — >= 3 consecutive down days
        # ------------------------------------------------------------------
        if "decline_streak" in wide.columns:
            streaking = wide[wide["decline_streak"] >= _DECLINE_STREAK_MIN_DAYS].dropna(subset=["decline_streak"])
            sub = streaking.nlargest(top_n, "decline_streak")
            rows = [
                {
                    "ticker": r.ticker,
                    "company_name": r.company_name,
                    "sector": r.sector,
                    "decline_streak": int(r.decline_streak),
                    "mom_5d": getattr(r, "mom_5d", float("nan")),
                    "mdd_252d": getattr(r, "mdd_252d", float("nan")),
                }
                for r in sub.itertuples(index=False)
            ]
            sections.append({"name": "decline_streaks", "label": f"DECLINE STREAKS (>= {_DECLINE_STREAK_MIN_DAYS} Consecutive Down Days)", "rows": rows})

        # ------------------------------------------------------------------
        # 5. VOLATILITY ALERTS — annualised 20-day vol >= 50%
        # ------------------------------------------------------------------
        if "vol_20d_ann252" in wide.columns:
            alert = wide[wide["vol_20d_ann252"] >= _VOL_ALERT_MIN].dropna(subset=["vol_20d_ann252"])
            sub = alert.nlargest(top_n, "vol_20d_ann252")
            rows = [
                {
                    "ticker": r.ticker,
                    "company_name": r.company_name,
                    "sector": r.sector,
                    "vol_20d_ann252": r.vol_20d_ann252,
                    "mom_5d": getattr(r, "mom_5d", float("nan")),
                }
                for r in sub.itertuples(index=False)
            ]
            sections.append({"name": "volatility_alerts", "label": f"VOLATILITY ALERTS (20d Ann Vol >= {_VOL_ALERT_MIN*100:.0f}%)", "rows": rows})

        # ------------------------------------------------------------------
        # 6. SECTOR SUMMARY — average 5d return by sector
        # ------------------------------------------------------------------
        if "mom_5d" in wide.columns and "sector" in wide.columns:
            sector_df = (
                wide.dropna(subset=["mom_5d", "sector"])
                .groupby("sector")
                .agg(
                    count=("ticker", "count"),
                    avg_mom_5d=("mom_5d", "mean"),
                    avg_mom_21d=("mom_21d", "mean"),
                )
                .reset_index()
                .sort_values("avg_mom_5d", ascending=False)
            )

            # Top 3 tickers per sector (by mom_5d)
            top_tickers = (
                wide.dropna(subset=["mom_5d", "sector"])
                .sort_values("mom_5d", ascending=False)
                .groupby("sector")["ticker"]
                .apply(lambda x: ", ".join(x.head(3)))
                .reset_index()
                .rename(columns={"ticker": "top_tickers"})
            )
            sector_df = sector_df.merge(top_tickers, on="sector", how="left")

            rows = [
                {
                    "sector": r.sector,
                    "count": int(r.count),
                    "avg_mom_5d": r.avg_mom_5d,
                    "avg_mom_21d": r.avg_mom_21d if not pd.isna(r.avg_mom_21d) else float("nan"),
                    "top_tickers": r.top_tickers,
                }
                for r in sector_df.itertuples(index=False)
            ]
            sections.append({"name": "sector_summary", "label": "SECTOR SUMMARY (Sorted by Avg 5d Return)", "rows": rows})

        return {"date": date, "sections": sections, "raw_df": wide}

    finally:
        if _owns_conn:
            conn.close()


def format_briefing(data: dict) -> str:
    """Convert a generate_briefing() result into a human-readable string."""
    date = data.get("date", "unknown")
    sections = data.get("sections", [])

    width = 72
    border = "=" * width
    lines = [
        border,
        f"  DAILY MARKET BRIEFING  —  {date}",
        border,
    ]

    if not sections:
        lines.append("  (no data available)")
        lines.append(border)
        return "\n".join(lines)

    section_icons = {
        "momentum_leaders":  "MOMENTUM LEADERS",
        "momentum_laggards": "MOMENTUM LAGGARDS",
        "volume_spikes":     "VOLUME SPIKES",
        "decline_streaks":   "DECLINE STREAKS",
        "volatility_alerts": "VOLATILITY ALERTS",
        "sector_summary":    "SECTOR SUMMARY",
    }

    for sec in sections:
        name = sec["name"]
        label = sec["label"]
        rows = sec["rows"]

        lines.append(f"\n{'─' * width}")
        lines.append(f"  {label}")
        lines.append(f"{'─' * width}")

        if not rows:
            lines.append("  (no entries)")
            continue

        if name in ("momentum_leaders", "momentum_laggards"):
            lines.append(f"  {'#':>3}  {'Ticker':<8}  {'Company':<24}  {'Sector':<16}  {'1d':>7}  {'5d':>7}  {'1m':>7}")
            for i, r in enumerate(rows, 1):
                lines.append(
                    f"  {i:>3}.  {r['ticker']:<8}  {(r['company_name'] or '')[:24]:<24}"
                    f"  {(r['sector'] or '')[:16]:<16}"
                    f"  {_fmt_pct(r['mom_1d']):>7}"
                    f"  {_fmt_pct(r['mom_5d']):>7}"
                    f"  {_fmt_pct(r['mom_21d']):>7}"
                )

        elif name == "volume_spikes":
            lines.append(f"  {'#':>3}  {'Ticker':<8}  {'Company':<24}  {'Sector':<16}  {'VolRatio':>8}  {'1d':>7}  {'5d':>7}")
            for i, r in enumerate(rows, 1):
                lines.append(
                    f"  {i:>3}.  {r['ticker']:<8}  {(r['company_name'] or '')[:24]:<24}"
                    f"  {(r['sector'] or '')[:16]:<16}"
                    f"  {_fmt_ratio(r['vol_ratio_20d']):>8}"
                    f"  {_fmt_pct(r['mom_1d']):>7}"
                    f"  {_fmt_pct(r['mom_5d']):>7}"
                )

        elif name == "decline_streaks":
            lines.append(f"  {'#':>3}  {'Ticker':<8}  {'Company':<24}  {'Sector':<16}  {'Streak':>7}  {'5d Ret':>7}  {'MDD':>8}")
            for i, r in enumerate(rows, 1):
                lines.append(
                    f"  {i:>3}.  {r['ticker']:<8}  {(r['company_name'] or '')[:24]:<24}"
                    f"  {(r['sector'] or '')[:16]:<16}"
                    f"  {_fmt_streak(r['decline_streak']):>7}"
                    f"  {_fmt_pct(r['mom_5d']):>7}"
                    f"  {_fmt_pct(r['mdd_252d']):>8}"
                )

        elif name == "volatility_alerts":
            lines.append(f"  {'#':>3}  {'Ticker':<8}  {'Company':<24}  {'Sector':<16}  {'Vol20d':>7}  {'5d Ret':>7}")
            for i, r in enumerate(rows, 1):
                lines.append(
                    f"  {i:>3}.  {r['ticker']:<8}  {(r['company_name'] or '')[:24]:<24}"
                    f"  {(r['sector'] or '')[:16]:<16}"
                    f"  {_fmt_pct(r['vol_20d_ann252']):>7}"
                    f"  {_fmt_pct(r['mom_5d']):>7}"
                )

        elif name == "sector_summary":
            lines.append(f"  {'Sector':<22}  {'N':>5}  {'Avg5d':>7}  {'Avg1m':>7}  Top Tickers")
            for r in rows:
                lines.append(
                    f"  {(r['sector'] or 'Unknown')[:22]:<22}"
                    f"  {r['count']:>5}"
                    f"  {_fmt_pct(r['avg_mom_5d']):>7}"
                    f"  {_fmt_pct(r['avg_mom_21d']):>7}"
                    f"  {r.get('top_tickers', '')}"
                )

    lines.append(f"\n{border}\n")
    return "\n".join(lines)


def run_briefing(date: Optional[str] = None, top_n: int = 20) -> str:
    """
    Orchestrator: generate + log + return the formatted briefing string.
    Called from daily_tasks.py after compute_all_factors().
    """
    conn = get_db_connection()
    if not conn:
        log.error("failed to get db connection")
        return ""

    try:
        data = generate_briefing(conn=conn, date=date, top_n=top_n)
        text = format_briefing(data)

        briefing_date = data.get("date") or "unknown"
        log_dir = get_config_value("log.log_dir", "logs")
        out_dir = os.path.join(log_dir, "briefings")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"{briefing_date}.log")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(text)
        log.info(f"[briefing] written to {out_path}")

        return text
    finally:
        conn.close()
