"""
测试 rw_universe.py - Universe 管理 CRUD 操作
使用 Mock 避免影响真实数据库
"""

import pytest
from unittest.mock import MagicMock
from database.readwrite.rw_universe import (
    insert_universe_definition,
    create_universe_snapshot,
    add_universe_members,
    get_universe_id,
    get_latest_snapshot,
    get_snapshot_members,
    delete_universe
)


@pytest.fixture
def mock_conn():
    """Mock 数据库连接和游标"""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


class TestInsertUniverseDefinition:
    """测试 insert_universe_definition"""
    
    def test_insert_new_universe(self, mock_conn):
        """插入新 Universe 定义"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (1,)
        
        result = insert_universe_definition(
            conn, universe_key='SP500',
            display_name='S&P 500 Index',
            source_type='csv',
            source_ref='sp500.csv'
        )
        
        assert result == 1
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'INSERT INTO universe_definitions' in sql
    
    def test_insert_duplicate_updates(self, mock_conn):
        """重复插入相同 universe_key 应该更新"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (1,)
        
        result = insert_universe_definition(
            conn, universe_key='SP500',
            display_name='S&P 500 Updated',
            source_type='api'
        )
        
        sql = cursor.execute.call_args[0][0]
        assert 'ON CONFLICT' in sql
    
    def test_insert_without_source_ref(self, mock_conn):
        """不提供 source_ref 应该成功"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (2,)
        
        result = insert_universe_definition(
            conn, universe_key='CUSTOM',
            display_name='Custom Universe',
            source_type='manual'
        )
        
        assert result == 2


class TestCreateUniverseSnapshot:
    """测试 create_universe_snapshot"""
    
    def test_create_snapshot_with_file(self, mock_conn):
        """创建带文件路径的快照"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (100,)
        
        result = create_universe_snapshot(
            conn, universe_id=1, as_of_date='2024-01-15',
            file_path='/data/sp500_20240115.csv'
        )
        
        assert result == 100
        assert cursor.execute.called
    
    def test_create_snapshot_with_content(self, mock_conn):
        """创建带原始内容的快照"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (101,)
        
        result = create_universe_snapshot(
            conn, universe_id=1, as_of_date='2024-01-15',
            raw_content='AAPL,GOOGL,MSFT'
        )
        
        assert result == 101
    
    def test_create_snapshot_with_notes(self, mock_conn):
        """创建带备注的快照"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (102,)
        
        result = create_universe_snapshot(
            conn, universe_id=1, as_of_date='2024-01-15',
            notes='Q4 2024 rebalance'
        )
        
        assert result == 102
    
    def test_create_duplicate_snapshot_updates(self, mock_conn):
        """同一日期的快照应该更新"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (100,)
        
        result = create_universe_snapshot(
            conn, universe_id=1, as_of_date='2024-01-15',
            notes='Updated'
        )
        
        sql = cursor.execute.call_args[0][0]
        assert 'ON CONFLICT' in sql


class TestAddUniverseMembers:
    """测试 add_universe_members"""
    
    def test_add_members_to_snapshot(self, mock_conn):
        """添加成员到快照"""
        conn, cursor = mock_conn
        
        members = [
            {'instrument_id': 1, 'raw_ticker': 'AAPL'},
            {'instrument_id': 2, 'raw_ticker': 'GOOGL'},
            {'instrument_id': 3, 'raw_ticker': 'MSFT'}
        ]
        
        add_universe_members(conn, snapshot_id=100, members=members)
        
        # 应该调用 3 次插入 + 1 次更新 row_count
        assert cursor.execute.call_count == 4
    
    def test_add_members_with_weights(self, mock_conn):
        """添加带权重的成员"""
        conn, cursor = mock_conn
        
        members = [
            {'instrument_id': 1, 'weight_hint': 0.05, 'raw_ticker': 'AAPL'},
            {'instrument_id': 2, 'weight_hint': 0.04, 'raw_ticker': 'GOOGL'}
        ]
        
        add_universe_members(conn, snapshot_id=100, members=members)
        
        assert cursor.execute.call_count == 3  # 2 inserts + 1 update
    
    def test_add_empty_members(self, mock_conn):
        """添加空成员列表"""
        conn, cursor = mock_conn
        
        add_universe_members(conn, snapshot_id=100, members=[])
        
        # 应该只调用 1 次更新 row_count
        assert cursor.execute.call_count == 1
    
    def test_add_duplicate_members_updates(self, mock_conn):
        """重复添加相同成员应该更新"""
        conn, cursor = mock_conn
        
        members = [
            {'instrument_id': 1, 'weight_hint': 0.05, 'raw_ticker': 'AAPL'}
        ]
        
        add_universe_members(conn, snapshot_id=100, members=members)
        
        # 检查是否使用了 ON CONFLICT
        sql_calls = [call[0][0] for call in cursor.execute.call_args_list]
        assert any('ON CONFLICT' in sql for sql in sql_calls)


class TestGetUniverseId:
    """测试 get_universe_id"""
    
    def test_get_existing_universe(self, mock_conn):
        """获取存在的 Universe ID"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (1,)
        
        result = get_universe_id(conn, 'SP500')
        
        assert result == 1
        assert cursor.execute.called
    
    def test_get_nonexistent_universe(self, mock_conn):
        """获取不存在的 Universe 返回 None"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None
        
        result = get_universe_id(conn, 'UNKNOWN')
        
        assert result is None


class TestGetLatestSnapshot:
    """测试 get_latest_snapshot"""
    
    def test_get_latest_snapshot_exists(self, mock_conn):
        """获取最新快照"""
        conn, cursor = mock_conn
        cursor.description = [
            ('snapshot_id',), ('universe_id',), ('as_of_date',),
            ('file_path',), ('row_count',)
        ]
        cursor.fetchone.return_value = (100, 1, '2024-01-15', '/data/sp500.csv', 500)
        
        result = get_latest_snapshot(conn, universe_id=1)
        
        assert result is not None
        assert result['snapshot_id'] == 100
        assert result['row_count'] == 500
        sql = cursor.execute.call_args[0][0]
        assert 'ORDER BY as_of_date DESC' in sql
        assert 'LIMIT 1' in sql
    
    def test_get_latest_snapshot_no_data(self, mock_conn):
        """没有快照时返回 None"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None
        
        result = get_latest_snapshot(conn, universe_id=999)
        
        assert result is None


class TestGetSnapshotMembers:
    """测试 get_snapshot_members"""
    
    def test_get_members_with_instrument_info(self, mock_conn):
        """获取快照成员及资产信息"""
        conn, cursor = mock_conn
        cursor.description = [
            ('instrument_id',), ('ticker',), ('company_name',), ('sector',)
        ]
        cursor.fetchall.return_value = [
            (1, 'AAPL', 'Apple Inc.', 'Technology'),
            (2, 'GOOGL', 'Alphabet Inc.', 'Technology')
        ]
        
        result = get_snapshot_members(conn, snapshot_id=100)
        
        assert len(result) == 2
        sql = cursor.execute.call_args[0][0]
        assert 'JOIN instruments' in sql
    
    def test_get_members_empty_snapshot(self, mock_conn):
        """空快照返回空 DataFrame"""
        conn, cursor = mock_conn
        cursor.description = [('instrument_id',), ('ticker',)]
        cursor.fetchall.return_value = []
        
        result = get_snapshot_members(conn, snapshot_id=100)
        
        assert len(result) == 0


class TestDeleteUniverse:
    """测试 delete_universe"""
    
    def test_delete_universe_cascades(self, mock_conn):
        """删除 Universe 会级联删除快照和成员"""
        conn, cursor = mock_conn
        
        delete_universe(conn, universe_id=1)
        
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'DELETE FROM universe_definitions' in sql
    
    def test_delete_nonexistent_universe(self, mock_conn):
        """删除不存在的 Universe 不报错"""
        conn, cursor = mock_conn
        
        delete_universe(conn, universe_id=999)
        
        assert cursor.execute.called
