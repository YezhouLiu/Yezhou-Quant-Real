from typing import List, Dict, Optional
import pandas as pd
from utils.logger import get_logger

log = get_logger("rw_instruments")


def insert_instrument(
    conn,
    ticker: str,
    exchange: str = "UNKNOWN",
    asset_type: str = "Stock",
    currency: str = "USD",
    company_name: str = None,
    description: str = None,
    sector: str = None,
    industry: str = None,
    ipo_date: str = None,
    delist_date: str = None,
    status: str = "active",
    is_tradable: bool = False,
) -> int:
    """插入资产"""
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO instruments (ticker, exchange, asset_type, currency, company_name, 
                                description, sector, industry, ipo_date, delist_date, status, is_tradable)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, exchange) DO UPDATE SET
            company_name = EXCLUDED.company_name,
            description = EXCLUDED.description,
            sector = EXCLUDED.sector,
            industry = EXCLUDED.industry,
            ipo_date = EXCLUDED.ipo_date,
            delist_date = EXCLUDED.delist_date,
            status = EXCLUDED.status,
            is_tradable = EXCLUDED.is_tradable,
            updated_at = now()
        RETURNING instrument_id
    """,
        (
            ticker,
            exchange,
            asset_type,
            currency,
            company_name,
            description,
            sector,
            industry,
            ipo_date,
            delist_date,
            status,
            is_tradable,
        ),
    )

    instrument_id = cursor.fetchone()[0]
    log.info(f"[✔] 插入/更新资产: {ticker} (ID: {instrument_id})")
    return instrument_id


def get_instrument_id(conn, ticker: str, exchange: str = None) -> Optional[int]:
    """根据 ticker 查询 instrument_id。
    不传 exchange 时仅按 ticker 匹配，有多条记录时返回 instrument_id 最小的一条。
    """
    cursor = conn.cursor()
    if exchange is not None:
        cursor.execute(
            "SELECT instrument_id FROM instruments WHERE ticker = %s AND exchange = %s",
            (ticker, exchange),
        )
    else:
        cursor.execute(
            "SELECT instrument_id FROM instruments WHERE ticker = %s ORDER BY instrument_id LIMIT 1",
            (ticker,),
        )

    result = cursor.fetchone()
    return result[0] if result else None


def get_instrument_by_ticker(conn, ticker: str, exchange: str = None) -> Optional[Dict]:
    """根据 ticker 查询完整资产信息，exchange 可选。
    不传 exchange 时按 ticker 匹配，有多条记录时返回 instrument_id 最小的一条。
    """
    cursor = conn.cursor()
    if exchange is not None:
        cursor.execute(
            """
            SELECT instrument_id, ticker, exchange, asset_type, currency, company_name,
                   description, sector, industry, ipo_date, delist_date, status, is_tradable,
                   created_at, updated_at
            FROM instruments WHERE ticker = %s AND exchange = %s
            """,
            (ticker, exchange),
        )
    else:
        cursor.execute(
            """
            SELECT instrument_id, ticker, exchange, asset_type, currency, company_name,
                   description, sector, industry, ipo_date, delist_date, status, is_tradable,
                   created_at, updated_at
            FROM instruments WHERE ticker = %s ORDER BY instrument_id LIMIT 1
            """,
            (ticker,),
        )

    row = cursor.fetchone()
    if not row:
        return None

    return {
        "instrument_id": row[0],
        "ticker": row[1],
        "exchange": row[2],
        "asset_type": row[3],
        "currency": row[4],
        "company_name": row[5],
        "description": row[6],
        "sector": row[7],
        "industry": row[8],
        "ipo_date": row[9],
        "delist_date": row[10],
        "status": row[11],
        "is_tradable": row[12],
        "created_at": row[13],
        "updated_at": row[14],
    }


def get_instrument_by_id(conn, instrument_id: int) -> Optional[Dict]:
    """根据ID获取资产信息"""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT instrument_id, ticker, exchange, asset_type, currency, company_name,
               description, sector, industry, ipo_date, delist_date, status, is_tradable,
               created_at, updated_at
        FROM instruments WHERE instrument_id = %s
    """,
        (instrument_id,),
    )

    row = cursor.fetchone()
    if not row:
        return None

    return {
        "instrument_id": row[0],
        "ticker": row[1],
        "exchange": row[2],
        "asset_type": row[3],
        "currency": row[4],
        "company_name": row[5],
        "description": row[6],
        "sector": row[7],
        "industry": row[8],
        "ipo_date": row[9],
        "delist_date": row[10],
        "status": row[11],
        "is_tradable": row[12],
        "created_at": row[13],
        "updated_at": row[14],
    }


def get_all_instruments(
    conn, asset_type: str = None, is_tradable: bool = None
) -> pd.DataFrame:
    """获取所有资产"""
    query = "SELECT * FROM instruments WHERE 1=1"
    params = []

    if asset_type:
        query += " AND asset_type = %s"
        params.append(asset_type)

    if is_tradable is not None:
        query += " AND is_tradable = %s"
        params.append(is_tradable)

    query += " ORDER BY ticker"

    cursor = conn.cursor()
    cursor.execute(query, params)

    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def update_instrument_tradable(conn, instrument_id: int, is_tradable: bool):
    """更新资产可交易状态"""
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE instruments SET is_tradable = %s, updated_at = now()
        WHERE instrument_id = %s
    """,
        (is_tradable, instrument_id),
    )

    log.info(f"[✔] 更新资产 {instrument_id} 可交易状态: {is_tradable}")


def delete_instrument(conn, instrument_id: int):
    """删除资产（慎用，会级联删除相关数据）"""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM instruments WHERE instrument_id = %s", (instrument_id,))
    log.warning(f"[⚠] 删除资产 ID: {instrument_id}")


def batch_insert_instruments(conn, instruments: List[Dict]):
    """批量插入资产"""
    cursor = conn.cursor()

    for inst in instruments:
        cursor.execute(
            """
            INSERT INTO instruments (ticker, exchange, asset_type, currency, company_name, 
                                    description, sector, industry, ipo_date, status, is_tradable)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, exchange) DO UPDATE SET
                company_name = EXCLUDED.company_name,
                sector = EXCLUDED.sector,
                industry = EXCLUDED.industry,
                updated_at = now()
        """,
            (
                inst["ticker"],
                inst.get("exchange", "UNKNOWN"),
                inst.get("asset_type", "Stock"),
                inst.get("currency", "USD"),
                inst.get("company_name"),
                inst.get("description"),
                inst.get("sector"),
                inst.get("industry"),
                inst.get("ipo_date"),
                inst.get("status", "active"),
                inst.get("is_tradable", False),
            ),
        )

    log.info(f"[✔] 批量插入 {len(instruments)} 个资产")


def get_tradable_instrument_ids(conn) -> List[int]:
    """
    获取所有可交易资产的 instrument_id 列表
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT instrument_id
        FROM instruments
        WHERE is_tradable = TRUE
        ORDER BY instrument_id
        """
    )
    return [row[0] for row in cursor.fetchall()]


def get_tradable_tickers(conn) -> List[str]:
    """
    获取所有可交易资产的 ticker 列表（用于检查/展示）
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT ticker
        FROM instruments
        WHERE is_tradable = TRUE
        ORDER BY ticker
        """
    )
    return [row[0] for row in cursor.fetchall()]
