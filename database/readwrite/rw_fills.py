from typing import List, Dict, Optional
import pandas as pd
from utils.logger import get_logger

log = get_logger("rw_fills")


def insert_fill(conn, instrument_id: int, side: str, quantity: float, price: float,
               trade_time: str, commission: float = 0, fees: float = 0, fx_rate: float = None,
               notes: str = None, source: str = 'manual') -> int:
    """插入成交记录"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO fills (instrument_id, side, quantity, price, trade_time, 
                          commission, fees, fx_rate, notes, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING fill_id
    """, (instrument_id, side, quantity, price, trade_time, commission, fees, fx_rate, notes, source))
    
    fill_id = cursor.fetchone()[0]
    log.info(f"[✔] 插入成交记录: {fill_id}")
    return fill_id


def get_fills(conn, instrument_id: int = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """获取成交记录"""
    query = "SELECT * FROM fills WHERE 1=1"
    params = []
    
    if instrument_id:
        query += " AND instrument_id = %s"
        params.append(instrument_id)
    
    if start_date:
        query += " AND trade_time >= %s"
        params.append(start_date)
    
    if end_date:
        query += " AND trade_time <= %s"
        params.append(end_date)
    
    query += " ORDER BY trade_time DESC"
    
    cursor = conn.cursor()
    cursor.execute(query, params)
    
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def get_fill_by_id(conn, fill_id: int) -> Optional[Dict]:
    """根据ID获取成交记录"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM fills WHERE fill_id = %s", (fill_id,))
    
    row = cursor.fetchone()
    if not row:
        return None
    
    columns = [desc[0] for desc in cursor.description]
    return dict(zip(columns, row))


def delete_fill(conn, fill_id: int):
    """删除成交记录"""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM fills WHERE fill_id = %s", (fill_id,))
    log.warning(f"[⚠] 删除成交记录: {fill_id}")
