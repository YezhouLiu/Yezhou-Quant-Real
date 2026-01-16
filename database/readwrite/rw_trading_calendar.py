from typing import List, Dict
import pandas as pd
from utils.logger import get_logger

log = get_logger("rw_trading_calendar")


def insert_trading_day(conn, date: str, is_trading_day: bool, market: str = 'US', holiday_name: str = None):
    """插入交易日"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO trading_calendar (market, date, is_trading_day, holiday_name)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (market, date) DO UPDATE SET
            is_trading_day = EXCLUDED.is_trading_day,
            holiday_name = EXCLUDED.holiday_name
    """, (market, date, is_trading_day, holiday_name))


def batch_insert_trading_days(conn, days: List[Dict], market: str = 'US'):
    """批量插入交易日"""
    cursor = conn.cursor()
    
    for day in days:
        cursor.execute("""
            INSERT INTO trading_calendar (market, date, is_trading_day, holiday_name)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (market, date) DO NOTHING
        """, (market, day['date'], day['is_trading_day'], day.get('holiday_name')))
    
    log.info(f"[✔] 批量插入 {len(days)} 个交易日")


def is_trading_day(conn, date: str, market: str = 'US') -> bool:
    """检查是否为交易日"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT is_trading_day FROM trading_calendar
        WHERE market = %s AND date = %s
    """, (market, date))
    
    result = cursor.fetchone()
    return result[0] if result else False


def get_trading_days(conn, start_date: str, end_date: str, market: str = 'US') -> pd.DataFrame:
    """获取交易日列表"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM trading_calendar
        WHERE market = %s AND date BETWEEN %s AND %s AND is_trading_day = TRUE
        ORDER BY date
    """, (market, start_date, end_date))
    
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def get_next_trading_day(conn, date: str, market: str = 'US') -> str:
    """获取下一个交易日"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT date FROM trading_calendar
        WHERE market = %s AND date > %s AND is_trading_day = TRUE
        ORDER BY date
        LIMIT 1
    """, (market, date))
    
    result = cursor.fetchone()
    return result[0] if result else None


def get_prev_trading_day(conn, date: str, market: str = 'US') -> str:
    """获取上一个交易日"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT date FROM trading_calendar
        WHERE market = %s AND date < %s AND is_trading_day = TRUE
        ORDER BY date DESC
        LIMIT 1
    """, (market, date))
    
    result = cursor.fetchone()
    return result[0] if result else None
