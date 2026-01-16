"""
测试 rw_universe_members.py - Universe 成员管理 CRUD 操作
使用 Mock 避免影响真实数据库
"""

import pytest
from unittest.mock import MagicMock
from database.readwrite.rw_universe_members import (
    get_member_count,
    is_in_universe,
    get_member_tickers,
    update_member_weight,
    delete_member
)


@pytest.fixture
def mock_conn():
    """Mock 数据库连接和游标"""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


class TestGetMemberCount:
    """测试 get_member_count"""
    
    def test_get_member_count(self, mock_conn):
        """获取快照成员数量"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (500,)
        
        result = get_member_count(conn, snapshot_id=100)
        
        assert result == 500
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'COUNT(*)' in sql
    
    def test_get_member_count_zero(self, mock_conn):
        """空快照返回 0"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (0,)
        
        result = get_member_count(conn, snapshot_id=100)
        
        assert result == 0
    
    def test_get_member_count_different_snapshots(self, mock_conn):
        """不同快照有不同的成员数量"""
        conn, cursor = mock_conn
        cursor.fetchone.side_effect = [(500,), (503,), (495,)]
        
        count1 = get_member_count(conn, snapshot_id=100)
        count2 = get_member_count(conn, snapshot_id=99)
        count3 = get_member_count(conn, snapshot_id=98)
        
        assert count1 == 500
        assert count2 == 503
        assert count3 == 495


class TestIsInUniverse:
    """测试 is_in_universe"""
    
    def test_is_in_universe_true(self, mock_conn):
        """资产在 Universe 中返回 True"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (True,)
        
        result = is_in_universe(conn, snapshot_id=100, instrument_id=123)
        
        assert result is True
        sql = cursor.execute.call_args[0][0]
        assert 'EXISTS' in sql
    
    def test_is_in_universe_false(self, mock_conn):
        """资产不在 Universe 中返回 False"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (False,)
        
        result = is_in_universe(conn, snapshot_id=100, instrument_id=999)
        
        assert result is False
    
    def test_is_in_universe_different_snapshots(self, mock_conn):
        """同一资产在不同快照中的状态"""
        conn, cursor = mock_conn
        cursor.fetchone.side_effect = [(True,), (False,)]
        
        # 在新快照中
        result1 = is_in_universe(conn, snapshot_id=100, instrument_id=123)
        # 不在旧快照中（已移除）
        result2 = is_in_universe(conn, snapshot_id=98, instrument_id=123)
        
        assert result1 is True
        assert result2 is False


class TestGetMemberTickers:
    """测试 get_member_tickers"""
    
    def test_get_member_tickers(self, mock_conn):
        """获取快照中的所有 ticker"""
        conn, cursor = mock_conn
        cursor.fetchall.return_value = [
            ('AAPL',), ('GOOGL',), ('MSFT',), ('AMZN',), ('TSLA',)
        ]
        
        result = get_member_tickers(conn, snapshot_id=100)
        
        assert len(result) == 5
        assert 'AAPL' in result
        assert 'GOOGL' in result
        assert 'MSFT' in result
        sql = cursor.execute.call_args[0][0]
        assert 'JOIN instruments' in sql
        assert 'ORDER BY' in sql
    
    def test_get_member_tickers_empty(self, mock_conn):
        """空快照返回空列表"""
        conn, cursor = mock_conn
        cursor.fetchall.return_value = []
        
        result = get_member_tickers(conn, snapshot_id=100)
        
        assert result == []
    
    def test_get_member_tickers_ordered(self, mock_conn):
        """Ticker 按字母顺序返回"""
        conn, cursor = mock_conn
        cursor.fetchall.return_value = [
            ('AAPL',), ('AMZN',), ('GOOGL',), ('MSFT',), ('TSLA',)
        ]
        
        result = get_member_tickers(conn, snapshot_id=100)
        
        sql = cursor.execute.call_args[0][0]
        assert 'ORDER BY i.ticker' in sql
    
    def test_get_member_tickers_different_snapshots(self, mock_conn):
        """不同快照有不同的成员"""
        conn, cursor = mock_conn
        cursor.fetchall.side_effect = [
            [('AAPL',), ('GOOGL',), ('MSFT',)],
            [('AAPL',), ('GOOGL',), ('MSFT',), ('AMZN',)]
        ]
        
        tickers1 = get_member_tickers(conn, snapshot_id=100)
        tickers2 = get_member_tickers(conn, snapshot_id=99)
        
        assert len(tickers1) == 3
        assert len(tickers2) == 4


class TestUpdateMemberWeight:
    """测试 update_member_weight"""
    
    def test_update_member_weight(self, mock_conn):
        """更新成员权重"""
        conn, cursor = mock_conn
        
        update_member_weight(conn, snapshot_id=100, instrument_id=123, weight_hint=0.05)
        
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'UPDATE universe_members' in sql
        assert 'weight_hint' in sql
        params = cursor.execute.call_args[0][1]
        assert 0.05 in params
        assert 100 in params
        assert 123 in params
    
    def test_update_member_weight_to_zero(self, mock_conn):
        """更新权重为 0"""
        conn, cursor = mock_conn
        
        update_member_weight(conn, snapshot_id=100, instrument_id=123, weight_hint=0.0)
        
        params = cursor.execute.call_args[0][1]
        assert 0.0 in params
    
    def test_update_member_weight_large_value(self, mock_conn):
        """更新大权重值"""
        conn, cursor = mock_conn
        
        update_member_weight(conn, snapshot_id=100, instrument_id=123, weight_hint=0.25)
        
        params = cursor.execute.call_args[0][1]
        assert 0.25 in params
    
    def test_update_nonexistent_member(self, mock_conn):
        """更新不存在的成员不报错"""
        conn, cursor = mock_conn
        
        update_member_weight(conn, snapshot_id=100, instrument_id=999, weight_hint=0.05)
        
        assert cursor.execute.called


class TestDeleteMember:
    """测试 delete_member"""
    
    def test_delete_member(self, mock_conn):
        """删除成员"""
        conn, cursor = mock_conn
        
        delete_member(conn, snapshot_id=100, instrument_id=123)
        
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'DELETE FROM universe_members' in sql
        params = cursor.execute.call_args[0][1]
        assert 100 in params
        assert 123 in params
    
    def test_delete_nonexistent_member(self, mock_conn):
        """删除不存在的成员不报错"""
        conn, cursor = mock_conn
        
        delete_member(conn, snapshot_id=100, instrument_id=999)
        
        assert cursor.execute.called
    
    def test_delete_multiple_members(self, mock_conn):
        """删除多个成员"""
        conn, cursor = mock_conn
        
        delete_member(conn, snapshot_id=100, instrument_id=123)
        delete_member(conn, snapshot_id=100, instrument_id=124)
        delete_member(conn, snapshot_id=100, instrument_id=125)
        
        assert cursor.execute.call_count == 3
    
    def test_delete_member_from_specific_snapshot(self, mock_conn):
        """只从特定快照删除成员"""
        conn, cursor = mock_conn
        
        delete_member(conn, snapshot_id=100, instrument_id=123)
        
        # 确保使用了 snapshot_id 和 instrument_id
        params = cursor.execute.call_args[0][1]
        assert 100 in params
        assert 123 in params
