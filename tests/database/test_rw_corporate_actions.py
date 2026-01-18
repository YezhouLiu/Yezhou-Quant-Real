"""
测试 rw_corporate_actions.py - 公司行为数据 CRUD 操作
使用 Mock 避免影响真实数据库
"""

import pytest
from unittest.mock import MagicMock

from psycopg.types.json import Jsonb

from database.readwrite.rw_corporate_actions import (
    insert_corporate_action,
    batch_insert_corporate_actions,
    get_corporate_actions,
    get_latest_corporate_action_date,
    delete_corporate_actions,
)


@pytest.fixture
def mock_conn():
    """Mock 数据库连接和游标"""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.split())


class TestInsertCorporateAction:
    """测试 insert_corporate_action"""

    def test_insert_dividend(self, mock_conn):
        """插入现金分红"""
        conn, cursor = mock_conn

        insert_corporate_action(
            conn,
            instrument_id=123,
            action_date="2024-01-15",
            action_type="DIVIDEND_CASH",
            action_value=0.25,
            currency="USD",
            data_source="tiingo",
        )

        assert cursor.execute.called
        sql = _normalize_sql(cursor.execute.call_args[0][0])
        assert "INSERT INTO corporate_actions" in sql
        assert "ON CONFLICT" in sql

        params = cursor.execute.call_args[0][1]
        assert params[0] == 123
        assert params[1] == "2024-01-15"
        assert params[2] == "DIVIDEND_CASH"
        assert params[3] == 0.25
        assert params[4] == "USD"
        assert params[5] == "tiingo"

    def test_insert_split_with_payload(self, mock_conn):
        """插入拆股，带 raw_payload"""
        conn, cursor = mock_conn

        payload = {"ratio": 2.0, "sourceField": "example"}
        insert_corporate_action(
            conn,
            instrument_id=123,
            action_date="2024-02-01",
            action_type="SPLIT",
            action_value=2.0,
            raw_payload=payload,
        )

        params = cursor.execute.call_args[0][1]
        assert isinstance(params[6], Jsonb)
        assert params[6].obj == payload

    def test_update_existing_action(self, mock_conn):
        """更新已存在的公司行为（检查 ON CONFLICT 主键）"""
        conn, cursor = mock_conn

        insert_corporate_action(
            conn,
            instrument_id=123,
            action_date="2024-01-15",
            action_type="DIVIDEND_CASH",
            action_value=0.30,
        )

        sql = _normalize_sql(cursor.execute.call_args[0][0])
        assert (
            "ON CONFLICT (instrument_id, action_date, action_type) DO UPDATE SET" in sql
        )


class TestBatchInsertCorporateActions:
    """测试 batch_insert_corporate_actions"""

    def test_batch_insert_multiple(self, mock_conn):
        """批量插入多条公司行为"""
        conn, cursor = mock_conn

        actions = [
            {
                "instrument_id": 123,
                "action_date": "2024-01-15",
                "action_type": "DIVIDEND_CASH",
                "action_value": 0.25,
            },
            {
                "instrument_id": 123,
                "action_date": "2024-02-01",
                "action_type": "SPLIT",
                "action_value": 2.0,
            },
            {
                "instrument_id": 124,
                "action_date": "2024-03-01",
                "action_type": "SPECIAL_DIVIDEND",
                "action_value": 1.5,
            },
        ]

        batch_insert_corporate_actions(conn, actions)
        assert cursor.execute.call_count == 3

    def test_batch_insert_empty_list(self, mock_conn):
        """批量插入空列表不报错"""
        conn, cursor = mock_conn

        batch_insert_corporate_actions(conn, [])
        assert cursor.execute.call_count == 0

    def test_batch_insert_with_payload(self, mock_conn):
        """批量插入带 payload"""
        conn, cursor = mock_conn

        actions = [
            {
                "instrument_id": 123,
                "action_date": "2024-02-01",
                "action_type": "SPLIT",
                "action_value": 2.0,
                "raw_payload": {"ratio": 2.0},
            }
        ]

        batch_insert_corporate_actions(conn, actions)
        params = cursor.execute.call_args[0][1]
        assert isinstance(params[6], Jsonb)
        assert params[6].obj == {"ratio": 2.0}


class TestGetCorporateActions:
    """测试 get_corporate_actions"""

    def test_get_all_actions(self, mock_conn):
        """获取全部公司行为"""
        conn, cursor = mock_conn
        cursor.description = [("action_type",), ("action_date",), ("action_value",)]
        cursor.fetchall.return_value = [
            ("DIVIDEND_CASH", "2024-01-15", 0.25),
            ("SPLIT", "2024-02-01", 2.0),
        ]

        df = get_corporate_actions(conn, instrument_id=123)
        assert len(df) == 2

        sql = _normalize_sql(cursor.execute.call_args[0][0])
        assert "ORDER BY action_date DESC" in sql

    def test_get_actions_by_type(self, mock_conn):
        """按 action_type 过滤"""
        conn, cursor = mock_conn
        cursor.description = [("action_type",)]
        cursor.fetchall.return_value = [("DIVIDEND_CASH",)]

        df = get_corporate_actions(conn, instrument_id=123, action_type="DIVIDEND_CASH")
        params = cursor.execute.call_args[0][1]
        assert "DIVIDEND_CASH" in params

    def test_get_actions_date_range(self, mock_conn):
        """按日期区间过滤"""
        conn, cursor = mock_conn
        cursor.description = [("action_date",)]
        cursor.fetchall.return_value = [("2024-01-15",)]

        df = get_corporate_actions(
            conn,
            instrument_id=123,
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
        params = cursor.execute.call_args[0][1]
        assert "2024-01-01" in params
        assert "2024-12-31" in params

    def test_get_no_data(self, mock_conn):
        """没有数据返回空 DataFrame"""
        conn, cursor = mock_conn
        cursor.description = [("action_type",)]
        cursor.fetchall.return_value = []

        df = get_corporate_actions(conn, instrument_id=999)
        assert len(df) == 0


class TestGetLatestCorporateActionDate:
    """测试 get_latest_corporate_action_date"""

    def test_get_latest_exists(self, mock_conn):
        """存在最新日期"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = ("2024-02-01",)

        d = get_latest_corporate_action_date(conn, instrument_id=123)
        assert d == "2024-02-01"

        sql = _normalize_sql(cursor.execute.call_args[0][0])
        assert "ORDER BY action_date DESC" in sql
        assert "LIMIT 1" in sql

    def test_get_latest_by_type(self, mock_conn):
        """按类型取最新日期"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = ("2024-01-15",)

        d = get_latest_corporate_action_date(
            conn, instrument_id=123, action_type="DIVIDEND_CASH"
        )
        assert d == "2024-01-15"

        params = cursor.execute.call_args[0][1]
        assert "DIVIDEND_CASH" in params

    def test_get_latest_not_exists(self, mock_conn):
        """无记录返回 None"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None

        d = get_latest_corporate_action_date(conn, instrument_id=999)
        assert d is None


class TestDeleteCorporateActions:
    """测试 delete_corporate_actions"""

    def test_delete_all_actions(self, mock_conn):
        """删除全部公司行为"""
        conn, cursor = mock_conn

        delete_corporate_actions(conn, instrument_id=123)
        assert cursor.execute.called

        sql = _normalize_sql(cursor.execute.call_args[0][0])
        assert "DELETE FROM corporate_actions" in sql

    def test_delete_by_type(self, mock_conn):
        """按类型删除"""
        conn, cursor = mock_conn

        delete_corporate_actions(conn, instrument_id=123, action_type="DIVIDEND_CASH")
        params = cursor.execute.call_args[0][1]
        assert "DIVIDEND_CASH" in params
