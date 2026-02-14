from typing import List, Dict, Optional
import pandas as pd
from utils.logger import get_logger

log = get_logger("rw_exp_positions")


# ============================================================
# Insert
# ============================================================


def insert_exp_position(
    conn,
    date: str,
    instrument_id: int,
    quantity: float,
    buy_price: float,
    current_price: float,
    market_value: float,
):
    """插入单条实验持仓记录"""

    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO exp_positions
        (date, instrument_id, quantity, buy_price, current_price, market_value)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, instrument_id)
        DO UPDATE SET
            quantity = EXCLUDED.quantity,
            buy_price = EXCLUDED.buy_price,
            current_price = EXCLUDED.current_price,
            market_value = EXCLUDED.market_value
    """,
        (date, instrument_id, quantity, buy_price, current_price, market_value),
    )

    log.info(f"[✔] 插入/更新实验持仓: {date} - {instrument_id}")


def batch_insert_exp_positions(conn, rows: List[Dict]):
    """批量插入实验持仓"""

    cursor = conn.cursor()

    for row in rows:
        cursor.execute(
            """
            INSERT INTO exp_positions
            (date, instrument_id, quantity, buy_price, current_price, market_value)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, instrument_id)
            DO UPDATE SET
                quantity = EXCLUDED.quantity,
                buy_price = EXCLUDED.buy_price,
                current_price = EXCLUDED.current_price,
                market_value = EXCLUDED.market_value
        """,
            (
                row["date"],
                row["instrument_id"],
                row["quantity"],
                row["buy_price"],
                row["current_price"],
                row["market_value"],
            ),
        )

    log.info(f"[✔] 批量写入实验持仓: {len(rows)} 行")


# ============================================================
# Query
# ============================================================


def get_exp_positions(
    conn, date: str = None, instrument_id: int = None
) -> pd.DataFrame:
    """获取实验持仓记录"""

    query = "SELECT * FROM exp_positions WHERE 1=1"
    params = []

    if date:
        query += " AND date = %s"
        params.append(date)

    if instrument_id:
        query += " AND instrument_id = %s"
        params.append(instrument_id)

    query += " ORDER BY date DESC, market_value DESC"

    cursor = conn.cursor()
    cursor.execute(query, params)

    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def get_exp_nav(conn, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    计算实验 NAV（按日期聚合 market_value）
    """

    query = """
        SELECT
            date,
            SUM(market_value) AS nav
        FROM exp_positions
        WHERE 1=1
    """

    params = []

    if start_date:
        query += " AND date >= %s"
        params.append(start_date)

    if end_date:
        query += " AND date <= %s"
        params.append(end_date)

    query += """
        GROUP BY date
        ORDER BY date
    """

    cursor = conn.cursor()
    cursor.execute(query, params)

    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


# ============================================================
# Delete
# ============================================================


def delete_exp_positions_by_date(conn, date: str):
    """删除某日实验持仓"""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM exp_positions WHERE date = %s", (date,))
    log.warning(f"[⚠] 删除实验持仓: {date}")
