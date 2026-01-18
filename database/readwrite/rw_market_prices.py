from typing import List, Dict, Optional
import pandas as pd
from datetime import date
from utils.logger import get_logger

log = get_logger("rw_market_prices")


def insert_price(conn, instrument_id: int, date: str, close_price: float, adj_close: float,
                open_price: float = None, high_price: float = None, low_price: float = None,
                volume: int = None, adj_open: float = None, adj_high: float = None,
                adj_low: float = None, adj_volume: int = None, dividends: float = 0,
                stock_splits: float = 1, data_source: str = 'tiingo'):
    """插入单条价格数据"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO market_prices (
            instrument_id, date, open_price, high_price, low_price, close_price, volume,
            adj_open, adj_high, adj_low, adj_close, adj_volume, dividends, stock_splits, data_source
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (instrument_id, date) DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            adj_open = EXCLUDED.adj_open,
            adj_high = EXCLUDED.adj_high,
            adj_low = EXCLUDED.adj_low,
            adj_close = EXCLUDED.adj_close,
            adj_volume = EXCLUDED.adj_volume,
            dividends = EXCLUDED.dividends,
            stock_splits = EXCLUDED.stock_splits,
            ingested_at = now()
    """, (instrument_id, date, open_price, high_price, low_price, close_price, volume,
          adj_open, adj_high, adj_low, adj_close, adj_volume, dividends, stock_splits, data_source))


def batch_insert_prices(conn, prices: List[Dict]):
    """批量插入价格数据"""
    cursor = conn.cursor()
    
    for price in prices:
        cursor.execute("""
            INSERT INTO market_prices (
                instrument_id, date, open_price, high_price, low_price, close_price, volume,
                adj_open, adj_high, adj_low, adj_close, adj_volume, dividends, stock_splits, data_source
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (instrument_id, date) DO UPDATE SET
                close_price = EXCLUDED.close_price,
                adj_close = EXCLUDED.adj_close,
                volume = EXCLUDED.volume,
                ingested_at = now()
        """, (
            price['instrument_id'], price['date'],
            price.get('open_price'), price.get('high_price'), price.get('low_price'),
            price['close_price'], price.get('volume'),
            price.get('adj_open'), price.get('adj_high'), price.get('adj_low'),
            price['adj_close'], price.get('adj_volume'),
            price.get('dividends', 0), price.get('stock_splits', 1),
            price.get('data_source', 'tiingo')
        ))
    
    log.info(f"[✔] 批量插入 {len(prices)} 条价格数据")


def get_prices(conn, instrument_id: int, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """获取价格数据"""
    query = "SELECT * FROM market_prices WHERE instrument_id = %s"
    params = [instrument_id]
    
    if start_date:
        query += " AND date >= %s"
        params.append(start_date)
    
    if end_date:
        query += " AND date <= %s"
        params.append(end_date)
    
    query += " ORDER BY date"
    
    cursor = conn.cursor()
    cursor.execute(query, params)
    
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def get_latest_price(conn, instrument_id: int) -> Optional[Dict]:
    """获取最新价格"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM market_prices
        WHERE instrument_id = %s
        ORDER BY date DESC
        LIMIT 1
    """, (instrument_id,))
    
    row = cursor.fetchone()
    if not row:
        return None
    
    columns = [desc[0] for desc in cursor.description]
    return dict(zip(columns, row))


def get_price_on_date(conn, instrument_id: int, date: str) -> Optional[Dict]:
    """获取指定日期的价格"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM market_prices
        WHERE instrument_id = %s AND date = %s
    """, (instrument_id, date))
    
    row = cursor.fetchone()
    if not row:
        return None
    
    columns = [desc[0] for desc in cursor.description]
    return dict(zip(columns, row))


def delete_prices(conn, instrument_id: int, start_date: str = None, end_date: str = None):
    """删除价格数据"""
    query = "DELETE FROM market_prices WHERE instrument_id = %s"
    params = [instrument_id]
    
    if start_date:
        query += " AND date >= %s"
        params.append(start_date)
    
    if end_date:
        query += " AND date <= %s"
        params.append(end_date)
    
    cursor = conn.cursor()
    cursor.execute(query, params)
    log.warning(f"[⚠] 删除价格数据: instrument_id={instrument_id}")


def get_price_max_date(conn) -> Optional[str]:
    """获取 market_prices 最大日期（用于推进状态）"""
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(date) FROM market_prices;")
    row = cursor.fetchone()
    if not row or row[0] is None:
        return None
    d = row[0]
    return d.isoformat() if hasattr(d, "isoformat") else str(d)


def get_price_min_date(conn) -> Optional[date]:
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(date) FROM market_prices;")
    row = cursor.fetchone()
    if not row or row[0] is None:
        return None
    return row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0]))