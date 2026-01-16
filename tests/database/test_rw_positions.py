"""
测试 rw_positions.py - 持仓记录 CRUD 操作
使用 Mock 避免影响真实数据库
"""

import pytest
from unittest.mock import MagicMock
from database.readwrite.rw_positions import (
    upsert_position,
    batch_upsert_positions,
    get_positions_on_date,
    get_position_history,
    delete_positions
)


@pytest.fixture
def mock_conn():
    """Mock 数据库连接和游标"""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


class TestUpsertPosition:
    """测试 upsert_position"""
    
    def test_insert_new_position(self, mock_conn):
        """插入新持仓"""
        conn, cursor = mock_conn
        
        upsert_position(
            conn, date='2024-01-15', instrument_id=123,
            quantity=100, cost_basis=15000.0,
            last_price=155.0, market_value=15500.0
        )
        
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'INSERT INTO positions' in sql
        assert 'ON CONFLICT' in sql
    
    def test_update_existing_position(self, mock_conn):
        """更新已存在的持仓"""
        conn, cursor = mock_conn
        
        upsert_position(
            conn, date='2024-01-15', instrument_id=123,
            quantity=150, cost_basis=22500.0
        )
        
        sql = cursor.execute.call_args[0][0]
        assert 'DO UPDATE SET' in sql
    
    def test_upsert_minimal_fields(self, mock_conn):
        """只提供必需字段"""
        conn, cursor = mock_conn
        
        upsert_position(
            conn, date='2024-01-15', instrument_id=123, quantity=100
        )
        
        assert cursor.execute.called
    
    def test_upsert_zero_quantity(self, mock_conn):
        """插入零持仓（已清仓）"""
        conn, cursor = mock_conn
        
        upsert_position(
            conn, date='2024-01-15', instrument_id=123,
            quantity=0, market_value=0
        )
        
        params = cursor.execute.call_args[0][1]
        assert 0 in params
    
    def test_upsert_different_sources(self, mock_conn):
        """不同来源的持仓"""
        conn, cursor = mock_conn
        
        upsert_position(
            conn, date='2024-01-15', instrument_id=123,
            quantity=100, source='broker_report'
        )
        
        params = cursor.execute.call_args[0][1]
        assert 'broker_report' in params


class TestBatchUpsertPositions:
    """测试 batch_upsert_positions"""
    
    def test_batch_upsert_multiple_positions(self, mock_conn):
        """批量插入多个持仓"""
        conn, cursor = mock_conn
        
        positions = [
            {'date': '2024-01-15', 'instrument_id': 123, 'quantity': 100, 
             'cost_basis': 15000.0, 'last_price': 155.0, 'market_value': 15500.0},
            {'date': '2024-01-15', 'instrument_id': 124, 'quantity': 50,
             'cost_basis': 10000.0, 'last_price': 205.0, 'market_value': 10250.0},
            {'date': '2024-01-15', 'instrument_id': 125, 'quantity': 200,
             'cost_basis': 20000.0, 'last_price': 102.0, 'market_value': 20400.0}
        ]
        
        batch_upsert_positions(conn, positions)
        
        assert cursor.execute.call_count == 3
    
    def test_batch_upsert_with_defaults(self, mock_conn):
        """批量插入使用默认值"""
        conn, cursor = mock_conn
        
        positions = [
            {'date': '2024-01-15', 'instrument_id': 123, 'quantity': 100},
            {'date': '2024-01-15', 'instrument_id': 124, 'quantity': 50}
        ]
        
        batch_upsert_positions(conn, positions)
        
        assert cursor.execute.call_count == 2
    
    def test_batch_upsert_empty_list(self, mock_conn):
        """批量插入空列表不报错"""
        conn, cursor = mock_conn
        
        batch_upsert_positions(conn, [])
        
        assert cursor.execute.call_count == 0
    
    def test_batch_upsert_updates_existing(self, mock_conn):
        """批量更新已存在的持仓"""
        conn, cursor = mock_conn
        
        positions = [
            {'date': '2024-01-15', 'instrument_id': 123, 'quantity': 150,
             'last_price': 160.0, 'market_value': 24000.0}
        ]
        
        batch_upsert_positions(conn, positions)
        
        sql = cursor.execute.call_args[0][0]
        assert 'ON CONFLICT' in sql


class TestGetPositionsOnDate:
    """测试 get_positions_on_date"""
    
    def test_get_positions_on_date(self, mock_conn):
        """获取指定日期的所有持仓"""
        conn, cursor = mock_conn
        cursor.description = [
            ('date',), ('instrument_id',), ('ticker',), ('quantity',), ('market_value',)
        ]
        cursor.fetchall.return_value = [
            ('2024-01-15', 123, 'AAPL', 100, 15500.0),
            ('2024-01-15', 124, 'GOOGL', 50, 10250.0)
        ]
        
        result = get_positions_on_date(conn, date='2024-01-15')
        
        assert len(result) == 2
        sql = cursor.execute.call_args[0][0]
        assert 'JOIN instruments' in sql
        assert 'quantity > 0' in sql
        assert 'ORDER BY' in sql
    
    def test_get_positions_no_data(self, mock_conn):
        """指定日期没有持仓返回空 DataFrame"""
        conn, cursor = mock_conn
        cursor.description = [('date',), ('instrument_id',)]
        cursor.fetchall.return_value = []
        
        result = get_positions_on_date(conn, date='2024-01-15')
        
        assert len(result) == 0
    
    def test_get_positions_filters_zero_quantity(self, mock_conn):
        """自动过滤零持仓"""
        conn, cursor = mock_conn
        cursor.description = [('instrument_id',), ('quantity',)]
        cursor.fetchall.return_value = [(123, 100), (124, 50)]
        
        result = get_positions_on_date(conn, date='2024-01-15')
        
        sql = cursor.execute.call_args[0][0]
        assert 'quantity > 0' in sql


class TestGetPositionHistory:
    """测试 get_position_history"""
    
    def test_get_position_history_all(self, mock_conn):
        """获取某资产的所有持仓历史"""
        conn, cursor = mock_conn
        cursor.description = [('date',), ('quantity',), ('market_value',)]
        cursor.fetchall.return_value = [
            ('2024-01-15', 100, 15500.0),
            ('2024-01-16', 100, 15600.0),
            ('2024-01-17', 100, 15700.0)
        ]
        
        result = get_position_history(conn, instrument_id=123)
        
        assert len(result) == 3
        sql = cursor.execute.call_args[0][0]
        assert 'ORDER BY date' in sql
    
    def test_get_position_history_with_date_range(self, mock_conn):
        """获取指定日期范围的持仓历史"""
        conn, cursor = mock_conn
        cursor.description = [('date',), ('quantity',)]
        cursor.fetchall.return_value = [('2024-01-15', 100)]
        
        result = get_position_history(
            conn, instrument_id=123,
            start_date='2024-01-01',
            end_date='2024-01-31'
        )
        
        params = cursor.execute.call_args[0][1]
        assert '2024-01-01' in params
        assert '2024-01-31' in params
    
    def test_get_position_history_start_date_only(self, mock_conn):
        """只指定开始日期"""
        conn, cursor = mock_conn
        cursor.description = [('date',)]
        cursor.fetchall.return_value = []
        
        result = get_position_history(
            conn, instrument_id=123,
            start_date='2024-01-01'
        )
        
        sql = cursor.execute.call_args[0][0]
        assert 'date >=' in sql
    
    def test_get_position_history_no_data(self, mock_conn):
        """没有持仓历史返回空 DataFrame"""
        conn, cursor = mock_conn
        cursor.description = [('date',)]
        cursor.fetchall.return_value = []
        
        result = get_position_history(conn, instrument_id=999)
        
        assert len(result) == 0


class TestDeletePositions:
    """测试 delete_positions"""
    
    def test_delete_positions_on_date(self, mock_conn):
        """删除指定日期的所有持仓"""
        conn, cursor = mock_conn
        
        delete_positions(conn, date='2024-01-15')
        
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'DELETE FROM positions' in sql
        params = cursor.execute.call_args[0][1]
        assert '2024-01-15' in params
    
    def test_delete_positions_multiple_dates(self, mock_conn):
        """删除多个日期的持仓"""
        conn, cursor = mock_conn
        
        delete_positions(conn, date='2024-01-15')
        delete_positions(conn, date='2024-01-16')
        
        assert cursor.execute.call_count == 2
