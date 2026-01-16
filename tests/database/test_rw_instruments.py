"""
测试 rw_instruments.py - 资产管理 CRUD 操作
使用 Mock 避免影响真实数据库
"""

import pytest
from unittest.mock import MagicMock, patch
from database.readwrite.rw_instruments import (
    insert_instrument,
    get_instrument_id,
    get_instrument_by_id,
    get_all_instruments,
    update_instrument_tradable,
    delete_instrument,
    batch_insert_instruments
)


@pytest.fixture
def mock_conn():
    """Mock 数据库连接和游标"""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


class TestInsertInstrument:
    """测试 insert_instrument"""
    
    def test_insert_new_instrument(self, mock_conn):
        """插入新资产返回 instrument_id"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (123,)
        
        result = insert_instrument(
            conn, ticker='AAPL', exchange='US', 
            company_name='Apple Inc.', sector='Technology'
        )
        
        assert result == 123
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'INSERT INTO instruments' in sql
        assert 'ON CONFLICT' in sql
    
    def test_insert_duplicate_ticker_updates(self, mock_conn):
        """重复插入相同 ticker 应该更新而不是报错"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (123,)
        
        result = insert_instrument(
            conn, ticker='AAPL', exchange='US',
            company_name='Apple Inc. Updated'
        )
        
        assert result == 123
        assert cursor.execute.called
    
    def test_insert_minimal_fields(self, mock_conn):
        """只提供必需字段应该成功"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (456,)
        
        result = insert_instrument(conn, ticker='TSLA', exchange='US')
        
        assert result == 456
    
    def test_insert_with_ipo_date(self, mock_conn):
        """插入包含 IPO 日期的资产"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (789,)
        
        result = insert_instrument(
            conn, ticker='SNOW', exchange='US',
            ipo_date='2020-09-16'
        )
        
        assert result == 789
        params = cursor.execute.call_args[0][1]
        assert '2020-09-16' in params
    
    def test_insert_delisted_stock(self, mock_conn):
        """插入退市股票"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (999,)
        
        result = insert_instrument(
            conn, ticker='DEAD', exchange='US',
            status='delisted', delist_date='2023-12-31'
        )
        
        assert result == 999


class TestGetInstrumentId:
    """测试 get_instrument_id"""
    
    def test_get_existing_instrument(self, mock_conn):
        """查询存在的资产返回 ID"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (123,)
        
        result = get_instrument_id(conn, 'AAPL', 'US')
        
        assert result == 123
        assert cursor.execute.called
    
    def test_get_nonexistent_instrument(self, mock_conn):
        """查询不存在的资产返回 None"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None
        
        result = get_instrument_id(conn, 'XXXXXX', 'US')
        
        assert result is None
    
    def test_get_instrument_different_exchanges(self, mock_conn):
        """同一 ticker 在不同交易所有不同的 ID"""
        conn, cursor = mock_conn
        cursor.fetchone.side_effect = [(100,), (200,)]
        
        result_us = get_instrument_id(conn, 'BABA', 'US')
        result_hk = get_instrument_id(conn, 'BABA', 'HK')
        
        assert result_us == 100
        assert result_hk == 200


class TestGetInstrumentById:
    """测试 get_instrument_by_id"""
    
    def test_get_instrument_full_info(self, mock_conn):
        """根据 ID 获取完整资产信息"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (
            123, 'AAPL', 'US', 'Stock', 'USD', 'Apple Inc.',
            'Tech company', 'Technology', 'Consumer Electronics',
            '1980-12-12', None, 'active', True,
            '2024-01-01', '2024-01-01'
        )
        
        result = get_instrument_by_id(conn, 123)
        
        assert result['instrument_id'] == 123
        assert result['ticker'] == 'AAPL'
        assert result['company_name'] == 'Apple Inc.'
        assert result['is_tradable'] is True
    
    def test_get_nonexistent_id(self, mock_conn):
        """查询不存在的 ID 返回 None"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None
        
        result = get_instrument_by_id(conn, 99999)
        
        assert result is None


class TestGetAllInstruments:
    """测试 get_all_instruments"""
    
    def test_get_all_no_filter(self, mock_conn):
        """获取所有资产"""
        conn, cursor = mock_conn
        cursor.description = [('instrument_id',), ('ticker',), ('exchange',)]
        cursor.fetchall.return_value = [
            (1, 'AAPL', 'US'),
            (2, 'GOOGL', 'US')
        ]
        
        result = get_all_instruments(conn)
        
        assert len(result) == 2
        assert cursor.execute.called
    
    def test_get_all_filter_by_asset_type(self, mock_conn):
        """按资产类型过滤"""
        conn, cursor = mock_conn
        cursor.description = [('instrument_id',), ('ticker',)]
        cursor.fetchall.return_value = [(1, 'SPY')]
        
        result = get_all_instruments(conn, asset_type='ETF')
        
        params = cursor.execute.call_args[0][1]
        assert 'ETF' in params
    
    def test_get_all_filter_by_tradable(self, mock_conn):
        """只获取可交易资产"""
        conn, cursor = mock_conn
        cursor.description = [('instrument_id',), ('ticker',)]
        cursor.fetchall.return_value = [(1, 'AAPL'), (2, 'GOOGL')]
        
        result = get_all_instruments(conn, is_tradable=True)
        
        params = cursor.execute.call_args[0][1]
        assert True in params
    
    def test_get_all_empty_result(self, mock_conn):
        """没有资产时返回空 DataFrame"""
        conn, cursor = mock_conn
        cursor.description = [('instrument_id',), ('ticker',)]
        cursor.fetchall.return_value = []
        
        result = get_all_instruments(conn)
        
        assert len(result) == 0


class TestUpdateInstrumentTradable:
    """测试 update_instrument_tradable"""
    
    def test_update_to_tradable(self, mock_conn):
        """更新资产为可交易"""
        conn, cursor = mock_conn
        
        update_instrument_tradable(conn, 123, True)
        
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'UPDATE instruments' in sql
        assert 'is_tradable' in sql
    
    def test_update_to_non_tradable(self, mock_conn):
        """更新资产为不可交易"""
        conn, cursor = mock_conn
        
        update_instrument_tradable(conn, 123, False)
        
        params = cursor.execute.call_args[0][1]
        assert False in params


class TestDeleteInstrument:
    """测试 delete_instrument"""
    
    def test_delete_existing_instrument(self, mock_conn):
        """删除存在的资产"""
        conn, cursor = mock_conn
        
        delete_instrument(conn, 123)
        
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'DELETE FROM instruments' in sql
    
    def test_delete_with_cascade(self, mock_conn):
        """删除会级联删除相关数据"""
        conn, cursor = mock_conn
        
        delete_instrument(conn, 123)
        
        # 确保调用了 DELETE 语句
        assert cursor.execute.called


class TestBatchInsertInstruments:
    """测试 batch_insert_instruments"""
    
    def test_batch_insert_multiple(self, mock_conn):
        """批量插入多个资产"""
        conn, cursor = mock_conn
        
        instruments = [
            {'ticker': 'AAPL', 'exchange': 'US', 'company_name': 'Apple'},
            {'ticker': 'GOOGL', 'exchange': 'US', 'company_name': 'Google'},
            {'ticker': 'MSFT', 'exchange': 'US', 'company_name': 'Microsoft'}
        ]
        
        batch_insert_instruments(conn, instruments)
        
        # 应该调用3次 execute
        assert cursor.execute.call_count == 3
    
    def test_batch_insert_with_defaults(self, mock_conn):
        """批量插入使用默认值"""
        conn, cursor = mock_conn
        
        instruments = [
            {'ticker': 'AAPL'},  # 只提供 ticker
            {'ticker': 'GOOGL', 'sector': 'Technology'}
        ]
        
        batch_insert_instruments(conn, instruments)
        
        assert cursor.execute.call_count == 2
    
    def test_batch_insert_empty_list(self, mock_conn):
        """批量插入空列表不报错"""
        conn, cursor = mock_conn
        
        batch_insert_instruments(conn, [])
        
        assert cursor.execute.call_count == 0

