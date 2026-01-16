"""
测试 rw_universe_snapshots.py - Universe 快照管理 CRUD 操作
使用 Mock 避免影响真实数据库
"""

import pytest
from unittest.mock import MagicMock
from database.readwrite.rw_universe_snapshots import (
    get_snapshot_by_id,
    get_all_snapshots,
    update_snapshot_notes,
    delete_snapshot
)


@pytest.fixture
def mock_conn():
    """Mock 数据库连接和游标"""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


class TestGetSnapshotById:
    """测试 get_snapshot_by_id"""
    
    def test_get_snapshot_exists(self, mock_conn):
        """根据 ID 获取快照"""
        conn, cursor = mock_conn
        cursor.description = [
            ('snapshot_id',), ('universe_id',), ('as_of_date',),
            ('file_path',), ('row_count',), ('notes',)
        ]
        cursor.fetchone.return_value = (
            100, 1, '2024-01-15', '/data/sp500.csv', 500, 'Q4 rebalance'
        )
        
        result = get_snapshot_by_id(conn, snapshot_id=100)
        
        assert result is not None
        assert result['snapshot_id'] == 100
        assert result['universe_id'] == 1
        assert result['row_count'] == 500
        assert result['notes'] == 'Q4 rebalance'
    
    def test_get_snapshot_not_exists(self, mock_conn):
        """获取不存在的快照返回 None"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None
        
        result = get_snapshot_by_id(conn, snapshot_id=999)
        
        assert result is None
    
    def test_get_snapshot_with_raw_content(self, mock_conn):
        """获取包含原始内容的快照"""
        conn, cursor = mock_conn
        cursor.description = [
            ('snapshot_id',), ('raw_content',), ('file_path',)
        ]
        cursor.fetchone.return_value = (100, 'AAPL,GOOGL,MSFT', None)
        
        result = get_snapshot_by_id(conn, snapshot_id=100)
        
        assert result['raw_content'] == 'AAPL,GOOGL,MSFT'


class TestGetAllSnapshots:
    """测试 get_all_snapshots"""
    
    def test_get_all_snapshots_for_universe(self, mock_conn):
        """获取 Universe 的所有快照"""
        conn, cursor = mock_conn
        cursor.description = [
            ('snapshot_id',), ('universe_id',), ('as_of_date',), ('row_count',)
        ]
        cursor.fetchall.return_value = [
            (100, 1, '2024-01-15', 500),
            (99, 1, '2023-12-31', 498),
            (98, 1, '2023-09-30', 495)
        ]
        
        result = get_all_snapshots(conn, universe_id=1)
        
        assert len(result) == 3
        sql = cursor.execute.call_args[0][0]
        assert 'ORDER BY as_of_date DESC' in sql
    
    def test_get_all_snapshots_no_data(self, mock_conn):
        """Universe 没有快照返回空 DataFrame"""
        conn, cursor = mock_conn
        cursor.description = [('snapshot_id',)]
        cursor.fetchall.return_value = []
        
        result = get_all_snapshots(conn, universe_id=999)
        
        assert len(result) == 0
    
    def test_get_all_snapshots_single(self, mock_conn):
        """Universe 只有一个快照"""
        conn, cursor = mock_conn
        cursor.description = [('snapshot_id',), ('as_of_date',)]
        cursor.fetchall.return_value = [(100, '2024-01-15')]
        
        result = get_all_snapshots(conn, universe_id=1)
        
        assert len(result) == 1
    
    def test_get_all_snapshots_ordered_by_date(self, mock_conn):
        """快照按日期降序排列"""
        conn, cursor = mock_conn
        cursor.description = [('snapshot_id',), ('as_of_date',)]
        cursor.fetchall.return_value = [
            (100, '2024-01-15'),
            (99, '2023-12-31'),
            (98, '2023-09-30')
        ]
        
        result = get_all_snapshots(conn, universe_id=1)
        
        sql = cursor.execute.call_args[0][0]
        assert 'ORDER BY as_of_date DESC' in sql


class TestUpdateSnapshotNotes:
    """测试 update_snapshot_notes"""
    
    def test_update_snapshot_notes(self, mock_conn):
        """更新快照备注"""
        conn, cursor = mock_conn
        
        update_snapshot_notes(conn, snapshot_id=100, notes='Updated rebalance notes')
        
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'UPDATE universe_snapshots' in sql
        assert 'notes' in sql
        params = cursor.execute.call_args[0][1]
        assert 'Updated rebalance notes' in params
        assert 100 in params
    
    def test_update_snapshot_notes_to_empty(self, mock_conn):
        """清空快照备注"""
        conn, cursor = mock_conn
        
        update_snapshot_notes(conn, snapshot_id=100, notes='')
        
        params = cursor.execute.call_args[0][1]
        assert '' in params
    
    def test_update_snapshot_notes_long_text(self, mock_conn):
        """更新长文本备注"""
        conn, cursor = mock_conn
        
        long_notes = 'This is a detailed rebalancing note. ' * 10
        update_snapshot_notes(conn, snapshot_id=100, notes=long_notes)
        
        params = cursor.execute.call_args[0][1]
        assert long_notes in params
    
    def test_update_nonexistent_snapshot(self, mock_conn):
        """更新不存在的快照不报错"""
        conn, cursor = mock_conn
        
        update_snapshot_notes(conn, snapshot_id=999, notes='Test')
        
        assert cursor.execute.called


class TestDeleteSnapshot:
    """测试 delete_snapshot"""
    
    def test_delete_snapshot_cascades(self, mock_conn):
        """删除快照会级联删除成员"""
        conn, cursor = mock_conn
        
        delete_snapshot(conn, snapshot_id=100)
        
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'DELETE FROM universe_snapshots' in sql
        params = cursor.execute.call_args[0][1]
        assert 100 in params
    
    def test_delete_nonexistent_snapshot(self, mock_conn):
        """删除不存在的快照不报错"""
        conn, cursor = mock_conn
        
        delete_snapshot(conn, snapshot_id=999)
        
        assert cursor.execute.called
    
    def test_delete_multiple_snapshots(self, mock_conn):
        """删除多个快照"""
        conn, cursor = mock_conn
        
        delete_snapshot(conn, snapshot_id=100)
        delete_snapshot(conn, snapshot_id=99)
        delete_snapshot(conn, snapshot_id=98)
        
        assert cursor.execute.call_count == 3
