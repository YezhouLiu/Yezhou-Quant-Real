"""
新增单只标的并下载历史价格。

对外暴露 add_instrument_and_download()，可在任意 task 中 import 调用。
直接运行本文件时，执行 __main__ 块里的示例。
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import date
from typing import Optional

from database.utils.db_utils import get_db_connection
from database.readwrite.rw_instruments import insert_instrument
from database.readwrite.rw_market_prices import batch_insert_prices
from data_download.input.price_downloader import (
    _build_session,
    fetch_tiingo_prices,
    transform_tiingo_price_data_to_db_format,
)
from utils.config_loader import get_config_value
from utils.logger import get_logger

log = get_logger("add_single_instrument")


def add_instrument_and_download(
    ticker: str,
    start_date: date,
    *,
    exchange: str = "NASDAQ",
    asset_type: str = "ETF",
    currency: str = "USD",
    company_name: Optional[str] = None,
    sector: Optional[str] = None,
    industry: Optional[str] = None,
    is_tradable: bool = True,
    end_date: Optional[date] = None,
) -> int:
    """
    将标的写入 instruments 表，并下载 start_date -> end_date（默认今天）的价格。

    Parameters
    ----------
    ticker       : 代码，如 "IBIT"
    start_date   : 下载起始日（建议填上市日）
    exchange     : 默认 "NASDAQ"
    asset_type   : "ETF" 或 "Stock"
    currency     : 默认 "USD"
    company_name : 全名，可选
    sector       : 行业，可选
    industry     : 细分行业，可选
    is_tradable  : 是否加入交易池，默认 True
    end_date     : 截止日，默认今天

    Returns
    -------
    instrument_id
    """
    if end_date is None:
        end_date = date.today()

    conn = get_db_connection()

    # 1. 写入 instruments
    instrument_id = insert_instrument(
        conn,
        ticker=ticker,
        exchange=exchange,
        asset_type=asset_type,
        currency=currency,
        company_name=company_name,
        sector=sector,
        industry=industry,
        is_tradable=is_tradable,
    )
    conn.commit()
    log.info(f"[{ticker}] instrument_id = {instrument_id}")

    # 2. 下载价格
    api_token = get_config_value("tiingo.api_key")
    session = _build_session()

    log.info(f"[{ticker}] Downloading prices {start_date} -> {end_date} ...")
    tiingo_data = fetch_tiingo_prices(ticker, start_date, end_date, api_token, session)

    if not tiingo_data:
        log.warning(f"[{ticker}] No price data returned")
        conn.close()
        return instrument_id

    records = transform_tiingo_price_data_to_db_format(tiingo_data, instrument_id)
    if records:
        batch_insert_prices(conn, records)
        conn.commit()
        log.info(f"[{ticker}] Inserted {len(records)} price records")
    else:
        log.warning(f"[{ticker}] transform returned no records")

    conn.close()
    return instrument_id


# ============================================================
# 在这里添加新标的，直接运行本文件即可
# ============================================================
if __name__ == "__main__":
    add_instrument_and_download(
        ticker="IBIT",
        start_date=date(2024, 1, 11),
        exchange="NASDAQ",
        asset_type="ETF",
        company_name="iShares Bitcoin Trust ETF",
        is_tradable=True,
    )
