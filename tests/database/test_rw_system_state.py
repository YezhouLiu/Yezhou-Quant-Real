"""
测试 rw_system_state.py - 系统状态 CRUD 操作
使用 Mock 避免影响真实数据库
"""

import pytest
from unittest.mock import MagicMock

from psycopg.types.json import Jsonb

from database.readwrite.rw_system_state import (
    set_state,
    get_state,
    delete_state,
    get_all_states
)


@pytest.fixture
def mock_conn():
    """Mock 数据库连接和游标"""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


def _normalize_sql(sql: str) -> str:
    """把 SQL 压成单行 + 单空格，避免换行/缩进导致测试脆弱"""
    return " ".join(sql.split())


class TestSetState:
    """测试 set_state"""

    def test_set_string_state(self, mock_conn):
        """设置字符串状态"""
        conn, cursor = mock_conn

        set_state(conn, key="last_update", value="2024-01-15")

        assert cursor.execute.called
        sql = _normalize_sql(cursor.execute.call_args[0][0])
        assert "INSERT INTO system_state" in sql
        assert "ON CONFLICT" in sql

        params = cursor.execute.call_args[0][1]
        assert params[0] == "last_update"
        assert isinstance(params[1], Jsonb)
        assert params[1].obj == "2024-01-15"

    def test_set_dict_state(self, mock_conn):
        """设置字典状态（通过 Jsonb 适配器写入）"""
        conn, cursor = mock_conn

        payload = {"version": "1.0", "debug": True}
        set_state(conn, key="config", value=payload)

        assert cursor.execute.called
        params = cursor.execute.call_args[0][1]

        assert params[0] == "config"
        assert isinstance(params[1], Jsonb)
        assert params[1].obj == payload

    def test_set_list_state(self, mock_conn):
        """设置列表状态（通过 Jsonb 适配器写入）"""
        conn, cursor = mock_conn

        payload = ["AAPL", "GOOGL", "MSFT"]
        set_state(conn, key="watchlist", value=payload)

        assert cursor.execute.called
        params = cursor.execute.call_args[0][1]
        assert params[0] == "watchlist"
        assert isinstance(params[1], Jsonb)
        assert params[1].obj == payload

    def test_set_numeric_state(self, mock_conn):
        """设置数值状态"""
        conn, cursor = mock_conn

        set_state(conn, key="total_trades", value=1234)

        assert cursor.execute.called
        params = cursor.execute.call_args[0][1]
        assert params[0] == "total_trades"
        assert isinstance(params[1], Jsonb)
        assert params[1].obj == 1234

    def test_update_existing_state(self, mock_conn):
        """更新已存在的状态（检查 ON CONFLICT DO UPDATE 结构）"""
        conn, cursor = mock_conn

        set_state(conn, key="last_update", value="2024-01-16")

        sql = _normalize_sql(cursor.execute.call_args[0][0])
        assert "ON CONFLICT (key) DO UPDATE SET" in sql
        assert "updated_at = now()" in sql


class TestGetState:
    """测试 get_state"""

    def test_get_existing_state(self, mock_conn):
        """获取存在的状态"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = ("2024-01-15",)

        result = get_state(conn, key="last_update")

        assert result == "2024-01-15"
        assert cursor.execute.called

        sql = _normalize_sql(cursor.execute.call_args[0][0])
        assert "SELECT value FROM system_state WHERE key = %s" in sql

    def test_get_dict_state(self, mock_conn):
        """获取字典状态（从 JSONB decode 回 Python dict）"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = ({"version": "1.0", "debug": True},)

        result = get_state(conn, key="config")

        assert result == {"version": "1.0", "debug": True}

    def test_get_list_state(self, mock_conn):
        """获取列表状态"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (["AAPL", "GOOGL", "MSFT"],)

        result = get_state(conn, key="watchlist")

        assert result == ["AAPL", "GOOGL", "MSFT"]

    def test_get_nonexistent_state_default(self, mock_conn):
        """获取不存在的状态返回默认值"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None

        result = get_state(conn, key="unknown", default="default_value")

        assert result == "default_value"

    def test_get_nonexistent_state_no_default(self, mock_conn):
        """获取不存在的状态且无默认值返回 None"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None

        result = get_state(conn, key="unknown")

        assert result is None

    def test_get_numeric_state(self, mock_conn):
        """获取数值状态"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (1234,)

        result = get_state(conn, key="total_trades")

        assert result == 1234


class TestDeleteState:
    """测试 delete_state"""

    def test_delete_existing_state(self, mock_conn):
        """删除存在的状态"""
        conn, cursor = mock_conn

        delete_state(conn, key="old_config")

        assert cursor.execute.called
        sql = _normalize_sql(cursor.execute.call_args[0][0])
        assert "DELETE FROM system_state WHERE key = %s" in sql

        params = cursor.execute.call_args[0][1]
        assert params == ("old_config",)

    def test_delete_nonexistent_state(self, mock_conn):
        """删除不存在的状态不报错"""
        conn, cursor = mock_conn

        delete_state(conn, key="nonexistent")

        assert cursor.execute.called

    def test_delete_multiple_states(self, mock_conn):
        """删除多个状态"""
        conn, cursor = mock_conn

        delete_state(conn, key="state1")
        delete_state(conn, key="state2")
        delete_state(conn, key="state3")

        assert cursor.execute.call_count == 3


class TestGetAllStates:
    """测试 get_all_states"""

    def test_get_all_states(self, mock_conn):
        """获取所有系统状态"""
        conn, cursor = mock_conn
        cursor.fetchall.return_value = [
            ("last_update", "2024-01-15"),
            ("total_trades", 1234),
            ("config", {"version": "1.0"}),
        ]

        result = get_all_states(conn)

        assert isinstance(result, dict)
        assert len(result) == 3
        assert result["last_update"] == "2024-01-15"
        assert result["total_trades"] == 1234
        assert result["config"] == {"version": "1.0"}

    def test_get_all_states_empty(self, mock_conn):
        """没有状态时返回空字典"""
        conn, cursor = mock_conn
        cursor.fetchall.return_value = []

        result = get_all_states(conn)

        assert result == {}

    def test_get_all_states_mixed_types(self, mock_conn):
        """获取包含多种类型的状态"""
        conn, cursor = mock_conn
        cursor.fetchall.return_value = [
            ("string_key", "value"),
            ("int_key", 123),
            ("dict_key", {"a": 1}),
            ("list_key", [1, 2, 3]),
        ]

        result = get_all_states(conn)

        assert len(result) == 4
        assert result["string_key"] == "value"
        assert result["int_key"] == 123
        assert result["dict_key"] == {"a": 1}
        assert result["list_key"] == [1, 2, 3]
