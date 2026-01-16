"""
测试 rw_fills.py - 成交记录 CRUD 操作
使用 Mock 避免影响真实数据库
"""

import pytest
from unittest.mock import MagicMock
from database.readwrite.rw_fills import (
    insert_fill,
    get_fills,
    get_fill_by_id,
    delete_fill
)


@pytest.fixture
def mock_conn():
    """Mock 数据库连接和游标"""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


class TestInsertFill:
    """测试 insert_fill"""
    
    def test_insert_buy_fill(self, mock_conn):
        """插入买入成交"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (1,)
        
        result = insert_fill(
            conn, instrument_id=123, side='BUY',
            quantity=100, price=150.0,
            trade_time='2024-01-15 09:30:00'
        )
        
        assert result == 1
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'INSERT INTO fills' in sql
    
    def test_insert_sell_fill(self, mock_conn):
        """插入卖出成交"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (2,)
        
        result = insert_fill(
            conn, instrument_id=123, side='SELL',
            quantity=50, price=155.0,
            trade_time='2024-01-15 15:30:00'
        )
        
        assert result == 2
    
    def test_insert_fill_with_commission(self, mock_conn):
        """插入包含佣金的成交"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (3,)
        
        result = insert_fill(
            conn, instrument_id=123, side='BUY',
            quantity=100, price=150.0,
            trade_time='2024-01-15 09:30:00',
            commission=1.5, fees=0.5
        )
        
        assert result == 3
        params = cursor.execute.call_args[0][1]
        assert 1.5 in params
        assert 0.5 in params
    
    def test_insert_fill_with_fx_rate(self, mock_conn):
        """插入包含汇率的成交（外币交易）"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (4,)
        
        result = insert_fill(
            conn, instrument_id=123, side='BUY',
            quantity=100, price=1000.0,
            trade_time='2024-01-15 09:30:00',
            fx_rate=7.2
        )
        
        assert result == 4
        params = cursor.execute.call_args[0][1]
        assert 7.2 in params
    
    def test_insert_fill_with_notes(self, mock_conn):
        """插入包含备注的成交"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (5,)
        
        result = insert_fill(
            conn, instrument_id=123, side='BUY',
            quantity=100, price=150.0,
            trade_time='2024-01-15 09:30:00',
            notes='Rebalance trade'
        )
        
        assert result == 5
    
    def test_insert_fill_different_sources(self, mock_conn):
        """插入来自不同来源的成交"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (6,)
        
        result = insert_fill(
            conn, instrument_id=123, side='BUY',
            quantity=100, price=150.0,
            trade_time='2024-01-15 09:30:00',
            source='broker_api'
        )
        
        params = cursor.execute.call_args[0][1]
        assert 'broker_api' in params


class TestGetFills:
    """测试 get_fills"""
    
    def test_get_all_fills(self, mock_conn):
        """获取所有成交记录"""
        conn, cursor = mock_conn
        cursor.description = [('fill_id',), ('side',), ('quantity',), ('price',)]
        cursor.fetchall.return_value = [
            (1, 'BUY', 100, 150.0),
            (2, 'SELL', 50, 155.0)
        ]
        
        result = get_fills(conn)
        
        assert len(result) == 2
        sql = cursor.execute.call_args[0][0]
        assert 'ORDER BY trade_time DESC' in sql
    
    def test_get_fills_by_instrument(self, mock_conn):
        """获取特定资产的成交记录"""
        conn, cursor = mock_conn
        cursor.description = [('fill_id',), ('instrument_id',)]
        cursor.fetchall.return_value = [(1, 123), (2, 123)]
        
        result = get_fills(conn, instrument_id=123)
        
        params = cursor.execute.call_args[0][1]
        assert 123 in params
    
    def test_get_fills_by_date_range(self, mock_conn):
        """获取日期范围内的成交"""
        conn, cursor = mock_conn
        cursor.description = [('fill_id',), ('trade_time',)]
        cursor.fetchall.return_value = [(1, '2024-01-15 10:00:00')]
        
        result = get_fills(
            conn,
            start_date='2024-01-01',
            end_date='2024-01-31'
        )
        
        params = cursor.execute.call_args[0][1]
        assert '2024-01-01' in params
        assert '2024-01-31' in params
    
    def test_get_fills_combined_filters(self, mock_conn):
        """使用多个过滤条件"""
        conn, cursor = mock_conn
        cursor.description = [('fill_id',)]
        cursor.fetchall.return_value = [(1,)]
        
        result = get_fills(
            conn,
            instrument_id=123,
            start_date='2024-01-01',
            end_date='2024-01-31'
        )
        
        params = cursor.execute.call_args[0][1]
        assert 123 in params
        assert '2024-01-01' in params
    
    def test_get_fills_no_data(self, mock_conn):
        """没有成交记录返回空 DataFrame"""
        conn, cursor = mock_conn
        cursor.description = [('fill_id',)]
        cursor.fetchall.return_value = []
        
        result = get_fills(conn, instrument_id=999)
        
        assert len(result) == 0


class TestGetFillById:
    """测试 get_fill_by_id"""
    
    def test_get_fill_exists(self, mock_conn):
        """根据 ID 获取成交记录"""
        conn, cursor = mock_conn
        cursor.description = [
            ('fill_id',), ('instrument_id',), ('side',), ('quantity',), ('price',)
        ]
        cursor.fetchone.return_value = (1, 123, 'BUY', 100, 150.0)
        
        result = get_fill_by_id(conn, fill_id=1)
        
        assert result is not None
        assert result['fill_id'] == 1
        assert result['side'] == 'BUY'
        assert result['quantity'] == 100
    
    def test_get_fill_not_exists(self, mock_conn):
        """查询不存在的 ID 返回 None"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None
        
        result = get_fill_by_id(conn, fill_id=999)
        
        assert result is None


class TestDeleteFill:
    """测试 delete_fill"""
    
    def test_delete_existing_fill(self, mock_conn):
        """删除存在的成交记录"""
        conn, cursor = mock_conn
        
        delete_fill(conn, fill_id=1)
        
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'DELETE FROM fills' in sql
        params = cursor.execute.call_args[0][1]
        assert 1 in params
    
    def test_delete_nonexistent_fill(self, mock_conn):
        """删除不存在的成交记录不报错"""
        conn, cursor = mock_conn
        
        delete_fill(conn, fill_id=999)
        
        assert cursor.execute.called
    
    def test_delete_multiple_fills(self, mock_conn):
        """删除多个成交记录"""
        conn, cursor = mock_conn
        
        delete_fill(conn, fill_id=1)
        delete_fill(conn, fill_id=2)
        delete_fill(conn, fill_id=3)
        
        assert cursor.execute.call_count == 3
