import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import requests

from utils.logger import get_logger

log = get_logger("fundamentals_downloader")

# =============================================================================
# SEC headers（必须真实，避免 ban）
# =============================================================================
SEC_USER_AGENT = "YezhouResearch/1.0 (contact: yezhouliu7@gmail.com)"

SEC_HEADERS_DATA = {
    "User-Agent": SEC_USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
    "Host": "data.sec.gov",
}
SEC_HEADERS_WWW = {
    "User-Agent": SEC_USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov",
}

# =============================================================================
# HTTP helpers（不吞错）
# =============================================================================
def _get_json(url: str, headers: Dict[str, str]) -> Any:
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def _cik_from_ticker(ticker: str) -> str:
    """
    从 SEC 官方 mapping 拿 CIK（10 位零填充）
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    data = _get_json(url, SEC_HEADERS_WWW)

    t = ticker.upper().strip()
    for _, obj in data.items():
        if obj["ticker"].upper() == t:
            return str(int(obj["cik_str"])).zfill(10)

    raise ValueError(f"Ticker not found in SEC mapping: {ticker}")


def fetch_companyfacts(ticker: str) -> Tuple[str, Dict[str, Any]]:
    cik = _cik_from_ticker(ticker)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    facts = _get_json(url, SEC_HEADERS_DATA)
    return cik, facts


# =============================================================================
# DB helpers（直接用 instruments / fundamental_data）
# =============================================================================
def get_instrument_id(conn, ticker: str, exchange: Optional[str] = None) -> int:
    cursor = conn.cursor()

    if exchange is None:
        cursor.execute(
            "SELECT instrument_id FROM instruments WHERE ticker = %s",
            (ticker,),
        )
        rows = cursor.fetchall()
        if len(rows) != 1:
            raise ValueError(
                f"ticker={ticker} matched {len(rows)} rows; please specify exchange"
            )
        return rows[0][0]

    cursor.execute(
        "SELECT instrument_id FROM instruments WHERE ticker = %s AND exchange = %s",
        (ticker, exchange),
    )
    row = cursor.fetchone()
    if not row:
        raise ValueError(f"instrument not found: {ticker} {exchange}")
    return row[0]


def upsert_fundamental_data(conn, rows: List[Dict[str, Any]]):
    if not rows:
        return

    cursor = conn.cursor()
    cursor.executemany(
        """
        INSERT INTO fundamental_data (
            instrument_id, report_date, metric_name,
            metric_value, period_type, period_start, period_end,
            currency, data_source
        )
        VALUES (
            %(instrument_id)s, %(report_date)s, %(metric_name)s,
            %(metric_value)s, %(period_type)s, %(period_start)s, %(period_end)s,
            %(currency)s, %(data_source)s
        )
        ON CONFLICT (instrument_id, report_date, metric_name, period_type)
        DO UPDATE SET
            metric_value = EXCLUDED.metric_value,
            period_start = EXCLUDED.period_start,
            period_end   = EXCLUDED.period_end,
            currency     = EXCLUDED.currency,
            data_source  = EXCLUDED.data_source,
            ingested_at  = now()
        """,
        rows,
    )


# =============================================================================
# 清洗规则
# =============================================================================
_ALLOWED_FORMS = {"10-Q", "10-K", "10-Q/A", "10-K/A"}
_ALLOWED_FP = {"FY", "Q1", "Q2", "Q3", "Q4"}


def _parse_date(s: Optional[str]) -> Optional[date]:
    if s is None:
        return None
    return date.fromisoformat(s[:10])


def _period_type_from_fp(fp: str) -> str:
    if fp == "FY":
        return "Annual"
    if fp in ("Q1", "Q2", "Q3", "Q4"):
        return "Quarterly"
    raise ValueError(f"Unsupported fp: {fp}")


def _is_duration_record(rec: Dict[str, Any]) -> bool:
    return rec.get("start") is not None and rec.get("end") is not None


def _duration_days(rec: Dict[str, Any]) -> int:
    s = _parse_date(rec["start"])
    e = _parse_date(rec["end"])
    return (e - s).days


def _keep_record(rec: Dict[str, Any]) -> bool:
    fp = rec.get("fp")
    if fp not in _ALLOWED_FP:
        return False

    if fp == "FY":
        return True

    if fp in ("Q1", "Q2", "Q3", "Q4"):
        if _is_duration_record(rec):
            d = _duration_days(rec)
            return 80 <= d <= 110
        return True

    return False


def normalize_and_dedupe_records(
    records: List[Dict[str, Any]], metric_name: str
) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for r in records:
        if r.get("form") not in _ALLOWED_FORMS:
            continue
        if r.get("fp") not in _ALLOWED_FP:
            continue
        if not _keep_record(r):
            continue
        if r.get("val") is None:
            continue
        filtered.append(r)

    best: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for r in filtered:
        end = r.get("end")
        fp = r.get("fp")
        filed = r.get("filed") or "0000-00-00"
        k = (metric_name, end, fp)
        if k not in best or filed > (best[k].get("filed") or "0000-00-00"):
            best[k] = r

    return list(best.values())


# =============================================================================
# 遍历所有 SEC 原生 tag（核心改动）
# =============================================================================
def iterate_all_fact_tags(facts: Dict[str, Any]):
    facts_root = facts.get("facts", {})
    for taxonomy, tags in facts_root.items():
        if not isinstance(tags, dict):
            continue
        for tag, obj in tags.items():
            units = obj.get("units")
            if not units:
                continue
            yield taxonomy, tag, units


# =============================================================================
# Public API：下载单 ticker（raw）
# =============================================================================
def download_one_ticker_fundamental_data(
    conn,
    *,
    ticker: str,
    exchange: Optional[str] = None,
    data_source: str = "sec_edgar",
) -> int:
    instrument_id = get_instrument_id(conn, ticker=ticker, exchange=exchange)

    cik, facts = fetch_companyfacts(ticker)
    entity = facts.get("entityName")
    log.info(f"[SEC] fetched companyfacts: {ticker} cik={cik} entity={entity}")

    to_insert: List[Dict[str, Any]] = []

    for taxonomy, tag, units in iterate_all_fact_tags(facts):
        metric_name = f"{taxonomy}.{tag}"

        for unit, records in units.items():
            if not isinstance(records, list):
                continue

            picked = normalize_and_dedupe_records(records, metric_name)
            if not picked:
                continue

            currency = unit.upper()

            for r in picked:
                period_type = _period_type_from_fp(r["fp"])

                to_insert.append(
                    {
                        "instrument_id": instrument_id,
                        "report_date": r["end"][:10],
                        "metric_name": metric_name,
                        "metric_value": float(r["val"]),
                        "period_type": period_type,
                        "period_start": r.get("start", None)[:10]
                        if r.get("start")
                        else None,
                        "period_end": r["end"][:10],
                        "currency": currency,
                        "data_source": data_source,
                    }
                )

    upsert_fundamental_data(conn, to_insert)
    log.info(f"[SEC] {ticker}: wrote {len(to_insert)} rows")
    return len(to_insert)
