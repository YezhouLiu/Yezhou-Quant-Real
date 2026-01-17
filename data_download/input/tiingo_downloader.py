"""
Tiingo API 工具函数
==================

提供 Tiingo API 的基础调用和数据转换功能。

语义约定（非常重要）：
- 返回 None：请求失败（HTTP 非 200，或 JSON 非预期）
- 返回 []：请求成功但无数据（200 且空列表）
- 返回 [..]：正常数据

Retry/Backoff：
- 不在本文件里做 retry/sleep；
- 由外部 requests.Session 的 HTTPAdapter(Retry) 负责（429/5xx）。

Author: YezhouLiu
Date: 2026-01-17
"""

from typing import Optional, List, Dict, Any
from datetime import date
import requests

from utils.logger import get_logger

log = get_logger("tiingo")


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
        # 关键：把错误信息露出来，否则你永远不知道是 401/403/404/429/5xx 还是参数问题
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


def transform_tiingo_to_db_format(
    tiingo_data: List[Dict[str, Any]],
    instrument_id: int,
) -> List[Dict[str, Any]]:
    """
    转换 Tiingo 响应为数据库格式。

    注意：这里默认不吞异常（早暴露问题）。
    如果你确实要容错，可以改为严格计数并在最后汇报/raise。
    """
    db_records: List[Dict[str, Any]] = []

    for record in tiingo_data:
        # 这里故意用直接索引：字段缺失就应当暴露
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
