"""
测试 rw_fundamental_data.py - 基本面数据 CRUD 操作
使用 Mock 避免影响真实数据库
"""

import pytest
from unittest.mock import MagicMock
from database.readwrite.rw_fundamental_data import (
    insert_fundamental,
    batch_insert_fundamentals,
    get_fundamentals,
    get_latest_fundamental,
    delete_fundamentals
)


@pytest.fixture
def mock_conn():
    """Mock 数据库连接和游标"""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


class TestInsertFundamental:
    """测试 insert_fundamental"""
    
    def test_insert_fundamental_annual(self, mock_conn):
        """插入年度基本面数据"""
        conn, cursor = mock_conn
        
        insert_fundamental(
            conn, instrument_id=123, metric_name='revenue',
            value=100000000.0, as_of_date='2023-12-31',
            period_type='annual'
        )
        
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'INSERT INTO fundamental_data' in sql
        assert 'ON CONFLICT' in sql
    
    def test_insert_fundamental_quarterly(self, mock_conn):
        """插入季度基本面数据"""
        conn, cursor = mock_conn
        
        insert_fundamental(
            conn, instrument_id=123, metric_name='revenue',
            value=25000000.0, as_of_date='2024-03-31',
            period_type='quarterly'
        )
        
        params = cursor.execute.call_args[0][1]
        assert 'quarterly' in params
    
    def test_insert_fundamental_with_source(self, mock_conn):
        """插入包含数据源的基本面数据"""
        conn, cursor = mock_conn
        
        insert_fundamental(
            conn, instrument_id=123, metric_name='pe_ratio',
            value=25.5, as_of_date='2024-01-15',
            source='yahoo'
        )
        
        params = cursor.execute.call_args[0][1]
        assert 'yahoo' in params
    
    def test_update_existing_fundamental(self, mock_conn):
        """更新已存在的基本面数据"""
        conn, cursor = mock_conn
        
        insert_fundamental(
            conn, instrument_id=123, metric_name='revenue',
            value=110000000.0, as_of_date='2023-12-31',
            period_type='annual'
        )
        
        sql = cursor.execute.call_args[0][0]
        assert 'ON CONFLICT (instrument_id, metric_name, as_of_date, period_type)' in sql
    
    def test_insert_different_metrics(self, mock_conn):
        """插入不同的财务指标"""
        conn, cursor = mock_conn
        
        metrics = [
            ('revenue', 100000000.0),
            ('net_income', 20000000.0),
            ('total_assets', 500000000.0),
            ('pe_ratio', 25.5),
            ('eps', 5.50)
        ]
        
        for metric_name, value in metrics:
            insert_fundamental(
                conn, instrument_id=123, metric_name=metric_name,
                value=value, as_of_date='2023-12-31'
            )
        
        assert cursor.execute.call_count == 5


class TestBatchInsertFundamentals:
    """测试 batch_insert_fundamentals"""
    
    def test_batch_insert_multiple_fundamentals(self, mock_conn):
        """批量插入多个基本面数据"""
        conn, cursor = mock_conn
        
        fundamentals = [
            {'instrument_id': 123, 'metric_name': 'revenue', 'value': 100000000.0, 'as_of_date': '2023-12-31'},
            {'instrument_id': 123, 'metric_name': 'net_income', 'value': 20000000.0, 'as_of_date': '2023-12-31'},
            {'instrument_id': 123, 'metric_name': 'eps', 'value': 5.50, 'as_of_date': '2023-12-31'}
        ]
        
        batch_insert_fundamentals(conn, fundamentals)
        
        assert cursor.execute.call_count == 3
    
    def test_batch_insert_with_period_types(self, mock_conn):
        """批量插入不同周期类型的数据"""
        conn, cursor = mock_conn
        
        fundamentals = [
            {'instrument_id': 123, 'metric_name': 'revenue', 'value': 100000000.0, 
             'as_of_date': '2023-12-31', 'period_type': 'annual'},
            {'instrument_id': 123, 'metric_name': 'revenue', 'value': 25000000.0,
             'as_of_date': '2024-03-31', 'period_type': 'quarterly'}
        ]
        
        batch_insert_fundamentals(conn, fundamentals)
        
        assert cursor.execute.call_count == 2
    
    def test_batch_insert_empty_list(self, mock_conn):
        """批量插入空列表不报错"""
        conn, cursor = mock_conn
        
        batch_insert_fundamentals(conn, [])
        
        assert cursor.execute.call_count == 0
    
    def test_batch_insert_with_sources(self, mock_conn):
        """批量插入包含数据源"""
        conn, cursor = mock_conn
        
        fundamentals = [
            {'instrument_id': 123, 'metric_name': 'revenue', 'value': 100000000.0,
             'as_of_date': '2023-12-31', 'source': 'yahoo'},
            {'instrument_id': 124, 'metric_name': 'revenue', 'value': 50000000.0,
             'as_of_date': '2023-12-31', 'source': 'bloomberg'}
        ]
        
        batch_insert_fundamentals(conn, fundamentals)
        
        assert cursor.execute.call_count == 2


class TestGetFundamentals:
    """测试 get_fundamentals"""
    
    def test_get_all_fundamentals(self, mock_conn):
        """获取某资产的所有基本面数据"""
        conn, cursor = mock_conn
        cursor.description = [
            ('metric_name',), ('value',), ('as_of_date',), ('period_type',)
        ]
        cursor.fetchall.return_value = [
            ('revenue', 100000000.0, '2023-12-31', 'annual'),
            ('net_income', 20000000.0, '2023-12-31', 'annual')
        ]
        
        result = get_fundamentals(conn, instrument_id=123)
        
        assert len(result) == 2
        sql = cursor.execute.call_args[0][0]
        assert 'ORDER BY as_of_date DESC' in sql
    
    def test_get_fundamentals_by_metric(self, mock_conn):
        """获取特定指标的数据"""
        conn, cursor = mock_conn
        cursor.description = [('metric_name',), ('value',), ('as_of_date',)]
        cursor.fetchall.return_value = [
            ('revenue', 100000000.0, '2023-12-31'),
            ('revenue', 90000000.0, '2022-12-31')
        ]
        
        result = get_fundamentals(conn, instrument_id=123, metric_name='revenue')
        
        params = cursor.execute.call_args[0][1]
        assert 'revenue' in params
    
    def test_get_fundamentals_date_range(self, mock_conn):
        """获取日期范围内的基本面数据"""
        conn, cursor = mock_conn
        cursor.description = [('as_of_date',), ('value',)]
        cursor.fetchall.return_value = [('2023-12-31', 100000000.0)]
        
        result = get_fundamentals(
            conn, instrument_id=123,
            start_date='2023-01-01',
            end_date='2023-12-31'
        )
        
        params = cursor.execute.call_args[0][1]
        assert '2023-01-01' in params
        assert '2023-12-31' in params
    
    def test_get_fundamentals_combined_filters(self, mock_conn):
        """使用多个过滤条件"""
        conn, cursor = mock_conn
        cursor.description = [('value',)]
        cursor.fetchall.return_value = [(100000000.0,)]
        
        result = get_fundamentals(
            conn, instrument_id=123,
            metric_name='revenue',
            start_date='2023-01-01',
            end_date='2023-12-31'
        )
        
        params = cursor.execute.call_args[0][1]
        assert 'revenue' in params
        assert '2023-01-01' in params
    
    def test_get_fundamentals_no_data(self, mock_conn):
        """没有数据返回空 DataFrame"""
        conn, cursor = mock_conn
        cursor.description = [('metric_name',)]
        cursor.fetchall.return_value = []
        
        result = get_fundamentals(conn, instrument_id=999)
        
        assert len(result) == 0


class TestGetLatestFundamental:
    """测试 get_latest_fundamental"""
    
    def test_get_latest_fundamental_exists(self, mock_conn):
        """获取最新的基本面指标"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (100000000.0,)
        
        result = get_latest_fundamental(conn, instrument_id=123, metric_name='revenue')
        
        assert result == 100000000.0
        sql = cursor.execute.call_args[0][0]
        assert 'ORDER BY as_of_date DESC' in sql
        assert 'LIMIT 1' in sql
    
    def test_get_latest_fundamental_not_exists(self, mock_conn):
        """获取不存在的指标返回 None"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None
        
        result = get_latest_fundamental(conn, instrument_id=999, metric_name='revenue')
        
        assert result is None
    
    def test_get_latest_different_metrics(self, mock_conn):
        """获取不同指标的最新值"""
        conn, cursor = mock_conn
        cursor.fetchone.side_effect = [
            (100000000.0,),  # revenue
            (20000000.0,),   # net_income
            (25.5,)          # pe_ratio
        ]
        
        revenue = get_latest_fundamental(conn, instrument_id=123, metric_name='revenue')
        net_income = get_latest_fundamental(conn, instrument_id=123, metric_name='net_income')
        pe_ratio = get_latest_fundamental(conn, instrument_id=123, metric_name='pe_ratio')
        
        assert revenue == 100000000.0
        assert net_income == 20000000.0
        assert pe_ratio == 25.5


class TestDeleteFundamentals:
    """测试 delete_fundamentals"""
    
    def test_delete_all_fundamentals(self, mock_conn):
        """删除某资产的所有基本面数据"""
        conn, cursor = mock_conn
        
        delete_fundamentals(conn, instrument_id=123)
        
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'DELETE FROM fundamental_data' in sql
    
    def test_delete_specific_metric(self, mock_conn):
        """只删除特定指标的数据"""
        conn, cursor = mock_conn
        
        delete_fundamentals(conn, instrument_id=123, metric_name='revenue')
        
        params = cursor.execute.call_args[0][1]
        assert 'revenue' in params
    
    def test_delete_nonexistent_data(self, mock_conn):
        """删除不存在的数据不报错"""
        conn, cursor = mock_conn
        
        delete_fundamentals(conn, instrument_id=999)
        
        assert cursor.execute.called
