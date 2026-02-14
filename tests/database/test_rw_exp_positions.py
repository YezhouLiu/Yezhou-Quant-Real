"""
测试 rw_exp_positions.py - 实验持仓 CRUD 操作
使用 Mock 避免影响真实数据库
"""

import pytest
from unittest.mock import MagicMock
from database.readwrite.rw_exp_positions import (
    insert_exp_position,
    batch_insert_exp_positions,
    get_exp_positions,
    get_exp_nav,
    delete_exp_positions_by_date,
)


# ============================================================
# Fixture
# ============================================================


@pytest.fixture
def mock_conn():
    """Mock 数据库连接和游标"""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


# ============================================================
# Insert Tests
# ============================================================


class TestInsertExpPosition:

    def test_insert_single_position(self, mock_conn):
        """插入单条持仓"""
        conn, cursor = mock_conn

        insert_exp_position(
            conn,
            date="2024-01-31",
            instrument_id=123,
            quantity=100,
            buy_price=150.0,
            current_price=155.0,
            market_value=15500.0,
        )

        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert "INSERT INTO exp_positions" in sql

    def test_insert_cash_position(self, mock_conn):
        """插入现金持仓（价格可为None）"""
        conn, cursor = mock_conn

        insert_exp_position(
            conn,
            date="2024-01-31",
            instrument_id=999,
            quantity=1000,
            buy_price=1.0,
            current_price=1.0,
            market_value=1000.0,
        )

        params = cursor.execute.call_args[0][1]
        assert 1000 in params
        assert 1.0 in params


class TestBatchInsertExpPositions:

    def test_batch_insert_multiple_rows(self, mock_conn):
        """批量插入多条持仓"""
        conn, cursor = mock_conn

        rows = [
            {
                "date": "2024-01-31",
                "instrument_id": 1,
                "quantity": 100,
                "buy_price": 10,
                "current_price": 12,
                "market_value": 1200,
            },
            {
                "date": "2024-01-31",
                "instrument_id": 2,
                "quantity": 200,
                "buy_price": 20,
                "current_price": 22,
                "market_value": 4400,
            },
        ]

        batch_insert_exp_positions(conn, rows)

        assert cursor.execute.call_count == 2


# ============================================================
# Query Tests
# ============================================================


class TestGetExpPositions:

    def test_get_all_positions(self, mock_conn):
        """获取全部持仓"""
        conn, cursor = mock_conn
        cursor.description = [
            ("date",),
            ("instrument_id",),
            ("quantity",),
            ("buy_price",),
            ("current_price",),
            ("market_value",),
        ]
        cursor.fetchall.return_value = [
            ("2024-01-31", 1, 100, 10, 12, 1200),
            ("2024-01-31", 2, 200, 20, 22, 4400),
        ]

        result = get_exp_positions(conn)

        assert len(result) == 2
        sql = cursor.execute.call_args[0][0]
        assert "ORDER BY date DESC" in sql

    def test_get_positions_by_date(self, mock_conn):
        """按日期查询"""
        conn, cursor = mock_conn
        cursor.description = [("date",)]
        cursor.fetchall.return_value = [("2024-01-31",)]

        result = get_exp_positions(conn, date="2024-01-31")

        params = cursor.execute.call_args[0][1]
        assert "2024-01-31" in params

    def test_get_positions_by_instrument(self, mock_conn):
        """按资产查询"""
        conn, cursor = mock_conn
        cursor.description = [("instrument_id",)]
        cursor.fetchall.return_value = [(123,)]

        result = get_exp_positions(conn, instrument_id=123)

        params = cursor.execute.call_args[0][1]
        assert 123 in params

    def test_get_positions_no_data(self, mock_conn):
        """无数据返回空 DataFrame"""
        conn, cursor = mock_conn
        cursor.description = [("date",)]
        cursor.fetchall.return_value = []

        result = get_exp_positions(conn, date="2025-01-01")

        assert len(result) == 0


class TestGetExpNav:

    def test_get_nav_basic(self, mock_conn):
        """计算 NAV"""
        conn, cursor = mock_conn
        cursor.description = [("date",), ("nav",)]
        cursor.fetchall.return_value = [("2024-01-31", 5600), ("2024-02-29", 5800)]

        result = get_exp_nav(conn)

        assert len(result) == 2
        sql = cursor.execute.call_args[0][0]
        assert "SUM(market_value)" in sql

    def test_get_nav_with_date_filter(self, mock_conn):
        """带日期范围的 NAV 查询"""
        conn, cursor = mock_conn
        cursor.description = [("date",), ("nav",)]
        cursor.fetchall.return_value = [("2024-01-31", 5600)]

        result = get_exp_nav(conn, start_date="2024-01-01", end_date="2024-01-31")

        params = cursor.execute.call_args[0][1]
        assert "2024-01-01" in params
        assert "2024-01-31" in params


# ============================================================
# Delete Tests
# ============================================================


class TestDeleteExpPositions:

    def test_delete_by_date(self, mock_conn):
        """删除某日持仓"""
        conn, cursor = mock_conn

        delete_exp_positions_by_date(conn, date="2024-01-31")

        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert "DELETE FROM exp_positions" in sql

    def test_delete_multiple_dates(self, mock_conn):
        """连续删除多天数据"""
        conn, cursor = mock_conn

        delete_exp_positions_by_date(conn, "2024-01-31")
        delete_exp_positions_by_date(conn, "2024-02-29")

        assert cursor.execute.call_count == 2
