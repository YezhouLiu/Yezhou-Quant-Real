"""
测试 rw_market_prices.py - 市场价格 CRUD 操作
使用 Mock 避免影响真实数据库
"""

import pytest
from unittest.mock import MagicMock
from database.readwrite.rw_market_prices import (
    insert_price,
    batch_insert_prices,
    get_prices,
    get_latest_price,
    get_price_on_date,
    delete_prices
)


@pytest.fixture
def mock_conn():
    """Mock 数据库连接和游标"""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


class TestInsertPrice:
    """测试 insert_price"""
    
    def test_insert_full_ohlcv(self, mock_conn):
        """插入完整的 OHLCV 数据"""
        conn, cursor = mock_conn
        
        insert_price(
            conn, instrument_id=123, date='2024-01-15',
            open_price=180.0, high_price=185.0, low_price=179.0,
            close_price=183.5, adj_close=183.5, volume=50000000
        )
        
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'INSERT INTO market_prices' in sql
        assert 'ON CONFLICT' in sql
    
    def test_insert_minimal_fields(self, mock_conn):
        """只插入必需字段（close_price 和 adj_close）"""
        conn, cursor = mock_conn
        
        insert_price(
            conn, instrument_id=123, date='2024-01-15',
            close_price=183.5, adj_close=183.5
        )
        
        assert cursor.execute.called
    
    def test_insert_with_dividends(self, mock_conn):
        """插入包含分红数据"""
        conn, cursor = mock_conn
        
        insert_price(
            conn, instrument_id=123, date='2024-01-15',
            close_price=100.0, adj_close=98.0,
            dividends=2.0
        )
        
        params = cursor.execute.call_args[0][1]
        assert 2.0 in params
    
    def test_insert_with_stock_split(self, mock_conn):
        """插入包含股票拆分数据"""
        conn, cursor = mock_conn
        
        insert_price(
            conn, instrument_id=123, date='2024-01-15',
            close_price=100.0, adj_close=50.0,
            stock_splits=2.0
        )
        
        params = cursor.execute.call_args[0][1]
        assert 2.0 in params
    
    def test_insert_duplicate_updates(self, mock_conn):
        """重复插入相同日期应该更新"""
        conn, cursor = mock_conn
        
        insert_price(
            conn, instrument_id=123, date='2024-01-15',
            close_price=100.0, adj_close=100.0
        )
        
        sql = cursor.execute.call_args[0][0]
        assert 'ON CONFLICT (instrument_id, date) DO UPDATE' in sql


class TestBatchInsertPrices:
    """测试 batch_insert_prices"""
    
    def test_batch_insert_multiple_days(self, mock_conn):
        """批量插入多天数据"""
        conn, cursor = mock_conn
        
        prices = [
            {'instrument_id': 123, 'date': '2024-01-01', 'close_price': 100.0, 'adj_close': 100.0},
            {'instrument_id': 123, 'date': '2024-01-02', 'close_price': 101.0, 'adj_close': 101.0},
            {'instrument_id': 123, 'date': '2024-01-03', 'close_price': 102.0, 'adj_close': 102.0}
        ]
        
        batch_insert_prices(conn, prices)
        
        assert cursor.execute.call_count == 3
    
    def test_batch_insert_with_volume(self, mock_conn):
        """批量插入包含成交量"""
        conn, cursor = mock_conn
        
        prices = [
            {
                'instrument_id': 123, 'date': '2024-01-01',
                'close_price': 100.0, 'adj_close': 100.0,
                'volume': 50000000
            }
        ]
        
        batch_insert_prices(conn, prices)
        
        params = cursor.execute.call_args[0][1]
        assert 50000000 in params
    
    def test_batch_insert_empty_list(self, mock_conn):
        """批量插入空列表不报错"""
        conn, cursor = mock_conn
        
        batch_insert_prices(conn, [])
        
        assert cursor.execute.call_count == 0
    
    def test_batch_insert_different_sources(self, mock_conn):
        """批量插入来自不同数据源"""
        conn, cursor = mock_conn
        
        prices = [
            {'instrument_id': 123, 'date': '2024-01-01', 'close_price': 100.0, 
             'adj_close': 100.0, 'data_source': 'tiingo'},
            {'instrument_id': 124, 'date': '2024-01-01', 'close_price': 200.0, 
             'adj_close': 200.0, 'data_source': 'yahoo'}
        ]
        
        batch_insert_prices(conn, prices)
        
        assert cursor.execute.call_count == 2


class TestGetPrices:
    """测试 get_prices"""
    
    def test_get_all_prices(self, mock_conn):
        """获取某资产的所有价格"""
        conn, cursor = mock_conn
        cursor.description = [('date',), ('close_price',)]
        cursor.fetchall.return_value = [
            ('2024-01-01', 100.0),
            ('2024-01-02', 101.0)
        ]
        
        result = get_prices(conn, instrument_id=123)
        
        assert len(result) == 2
        assert cursor.execute.called
    
    def test_get_prices_with_date_range(self, mock_conn):
        """获取指定日期范围的价格"""
        conn, cursor = mock_conn
        cursor.description = [('date',), ('close_price',)]
        cursor.fetchall.return_value = [('2024-01-15', 100.0)]
        
        result = get_prices(
            conn, instrument_id=123,
            start_date='2024-01-01', end_date='2024-01-31'
        )
        
        params = cursor.execute.call_args[0][1]
        assert '2024-01-01' in params
        assert '2024-01-31' in params
    
    def test_get_prices_start_date_only(self, mock_conn):
        """只指定开始日期"""
        conn, cursor = mock_conn
        cursor.description = [('date',)]
        cursor.fetchall.return_value = []
        
        result = get_prices(conn, instrument_id=123, start_date='2024-01-01')
        
        sql = cursor.execute.call_args[0][0]
        assert 'date >=' in sql
    
    def test_get_prices_no_data(self, mock_conn):
        """没有数据时返回空 DataFrame"""
        conn, cursor = mock_conn
        cursor.description = [('date',), ('close_price',)]
        cursor.fetchall.return_value = []
        
        result = get_prices(conn, instrument_id=999)
        
        assert len(result) == 0


class TestGetLatestPrice:
    """测试 get_latest_price"""
    
    def test_get_latest_price_exists(self, mock_conn):
        """获取最新价格"""
        conn, cursor = mock_conn
        cursor.description = [
            ('date',), ('close_price',), ('adj_close',), ('volume',)
        ]
        cursor.fetchone.return_value = ('2024-01-15', 183.5, 183.5, 50000000)
        
        result = get_latest_price(conn, instrument_id=123)
        
        assert result is not None
        assert result['close_price'] == 183.5
        sql = cursor.execute.call_args[0][0]
        assert 'ORDER BY date DESC' in sql
        assert 'LIMIT 1' in sql
    
    def test_get_latest_price_no_data(self, mock_conn):
        """没有价格数据返回 None"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None
        
        result = get_latest_price(conn, instrument_id=999)
        
        assert result is None


class TestGetPriceOnDate:
    """测试 get_price_on_date"""
    
    def test_get_price_on_specific_date(self, mock_conn):
        """获取特定日期的价格"""
        conn, cursor = mock_conn
        cursor.description = [('date',), ('close_price',), ('volume',)]
        cursor.fetchone.return_value = ('2024-01-15', 183.5, 50000000)
        
        result = get_price_on_date(conn, instrument_id=123, date='2024-01-15')
        
        assert result is not None
        assert result['close_price'] == 183.5
        params = cursor.execute.call_args[0][1]
        assert '2024-01-15' in params
    
    def test_get_price_on_nonexistent_date(self, mock_conn):
        """查询不存在的日期返回 None"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None
        
        result = get_price_on_date(conn, instrument_id=123, date='2025-12-31')
        
        assert result is None
    
    def test_get_price_on_holiday(self, mock_conn):
        """查询节假日（无交易）返回 None"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None
        
        result = get_price_on_date(conn, instrument_id=123, date='2024-07-04')
        
        assert result is None


class TestDeletePrices:
    """测试 delete_prices"""
    
    def test_delete_all_prices(self, mock_conn):
        """删除某资产的所有价格"""
        conn, cursor = mock_conn
        
        delete_prices(conn, instrument_id=123)
        
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'DELETE FROM market_prices' in sql
    
    def test_delete_prices_date_range(self, mock_conn):
        """删除指定日期范围的价格"""
        conn, cursor = mock_conn
        
        delete_prices(
            conn, instrument_id=123,
            start_date='2024-01-01', end_date='2024-01-31'
        )
        
        params = cursor.execute.call_args[0][1]
        assert '2024-01-01' in params
        assert '2024-01-31' in params
    
    def test_delete_prices_after_date(self, mock_conn):
        """删除指定日期之后的价格"""
        conn, cursor = mock_conn
        
        delete_prices(conn, instrument_id=123, start_date='2024-01-01')
        
        sql = cursor.execute.call_args[0][0]
        assert 'date >=' in sql
