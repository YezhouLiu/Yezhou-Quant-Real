from typing import List, Dict, Optional
import pandas as pd
from utils.logger import get_logger

log = get_logger("rw_positions")


def upsert_position(conn, date: str, instrument_id: int, quantity: float,
                   cost_basis: float = None, last_price: float = None,
                   market_value: float = None, source: str = 'computed'):
    """插入或更新持仓"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO positions (date, instrument_id, quantity, cost_basis, last_price, market_value, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, instrument_id) DO UPDATE SET
            quantity = EXCLUDED.quantity,
            cost_basis = EXCLUDED.cost_basis,
            last_price = EXCLUDED.last_price,
            market_value = EXCLUDED.market_value,
            updated_at = now()
    """, (date, instrument_id, quantity, cost_basis, last_price, market_value, source))
    
    log.info(f"[✔] 更新持仓: date={date}, instrument_id={instrument_id}, quantity={quantity}")


def batch_upsert_positions(conn, positions: List[Dict]):
    """批量插入或更新持仓"""
    cursor = conn.cursor()
    
    for pos in positions:
        cursor.execute("""
            INSERT INTO positions (date, instrument_id, quantity, cost_basis, last_price, market_value, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, instrument_id) DO UPDATE SET
                quantity = EXCLUDED.quantity,
                cost_basis = EXCLUDED.cost_basis,
                last_price = EXCLUDED.last_price,
                market_value = EXCLUDED.market_value,
                updated_at = now()
        """, (
            pos['date'], pos['instrument_id'], pos['quantity'],
            pos.get('cost_basis'), pos.get('last_price'), pos.get('market_value'),
            pos.get('source', 'computed')
        ))
    
    log.info(f"[✔] 批量更新 {len(positions)} 条持仓")


def get_positions_on_date(conn, date: str) -> pd.DataFrame:
    """获取指定日期的所有持仓"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT p.*, i.ticker, i.company_name
        FROM positions p
        JOIN instruments i ON p.instrument_id = i.instrument_id
        WHERE p.date = %s AND p.quantity > 0
        ORDER BY p.market_value DESC
    """, (date,))
    
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def get_position_history(conn, instrument_id: int, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """获取持仓历史"""
    query = "SELECT * FROM positions WHERE instrument_id = %s"
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


def delete_positions(conn, date: str):
    """删除指定日期的所有持仓"""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM positions WHERE date = %s", (date,))
    log.warning(f"[⚠] 删除持仓: date={date}")
