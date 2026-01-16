"""
测试 rw_trading_calendar.py - 交易日历 CRUD 操作
使用 Mock 避免影响真实数据库
"""

import pytest
from unittest.mock import MagicMock
from database.readwrite.rw_trading_calendar import (
    insert_trading_day,
    batch_insert_trading_days,
    is_trading_day,
    get_trading_days,
    get_next_trading_day,
    get_prev_trading_day
)


@pytest.fixture
def mock_conn():
    """Mock 数据库连接和游标"""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


class TestInsertTradingDay:
    """测试 insert_trading_day"""
    
    def test_insert_trading_day(self, mock_conn):
        """插入交易日"""
        conn, cursor = mock_conn
        
        insert_trading_day(conn, date='2024-01-15', is_trading_day=True)
        
        assert cursor.execute.called
        params = cursor.execute.call_args[0][1]
        assert '2024-01-15' in params
        assert True in params
    
    def test_insert_holiday(self, mock_conn):
        """插入节假日"""
        conn, cursor = mock_conn
        
        insert_trading_day(
            conn, date='2024-07-04',
            is_trading_day=False,
            holiday_name='Independence Day'
        )
        
        params = cursor.execute.call_args[0][1]
        assert False in params
        assert 'Independence Day' in params
    
    def test_insert_duplicate_updates(self, mock_conn):
        """重复插入相同日期应该更新"""
        conn, cursor = mock_conn
        
        insert_trading_day(conn, date='2024-01-15', is_trading_day=True)
        
        sql = cursor.execute.call_args[0][0]
        assert 'ON CONFLICT' in sql
    
    def test_insert_different_market(self, mock_conn):
        """插入不同市场的交易日"""
        conn, cursor = mock_conn
        
        insert_trading_day(conn, date='2024-01-15', is_trading_day=True, market='HK')
        
        params = cursor.execute.call_args[0][1]
        assert 'HK' in params


class TestBatchInsertTradingDays:
    """测试 batch_insert_trading_days"""
    
    def test_batch_insert_multiple_days(self, mock_conn):
        """批量插入多个交易日"""
        conn, cursor = mock_conn
        
        days = [
            {'date': '2024-01-15', 'is_trading_day': True},
            {'date': '2024-01-16', 'is_trading_day': True},
            {'date': '2024-01-17', 'is_trading_day': True}
        ]
        
        batch_insert_trading_days(conn, days)
        
        assert cursor.execute.call_count == 3
    
    def test_batch_insert_with_holidays(self, mock_conn):
        """批量插入包含节假日"""
        conn, cursor = mock_conn
        
        days = [
            {'date': '2024-01-01', 'is_trading_day': False, 'holiday_name': 'New Year'},
            {'date': '2024-01-02', 'is_trading_day': True}
        ]
        
        batch_insert_trading_days(conn, days)
        
        assert cursor.execute.call_count == 2
    
    def test_batch_insert_empty_list(self, mock_conn):
        """批量插入空列表不报错"""
        conn, cursor = mock_conn
        
        batch_insert_trading_days(conn, [])
        
        assert cursor.execute.call_count == 0
    
    def test_batch_insert_different_market(self, mock_conn):
        """批量插入不同市场"""
        conn, cursor = mock_conn
        
        days = [
            {'date': '2024-01-15', 'is_trading_day': True},
            {'date': '2024-01-16', 'is_trading_day': True}
        ]
        
        batch_insert_trading_days(conn, days, market='HK')
        
        # 检查是否使用了正确的市场
        params = cursor.execute.call_args[0][1]
        assert 'HK' in params


class TestIsTradingDay:
    """测试 is_trading_day"""
    
    def test_is_trading_day_true(self, mock_conn):
        """检查是交易日返回 True"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (True,)
        
        result = is_trading_day(conn, date='2024-01-15')
        
        assert result is True
        assert cursor.execute.called
    
    def test_is_trading_day_false(self, mock_conn):
        """检查是节假日返回 False"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (False,)
        
        result = is_trading_day(conn, date='2024-07-04')
        
        assert result is False
    
    def test_is_trading_day_no_data(self, mock_conn):
        """日期不存在返回 False"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None
        
        result = is_trading_day(conn, date='2025-12-31')
        
        assert result is False
    
    def test_is_trading_day_different_market(self, mock_conn):
        """检查不同市场的交易日"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (True,)
        
        result = is_trading_day(conn, date='2024-01-15', market='HK')
        
        params = cursor.execute.call_args[0][1]
        assert 'HK' in params


class TestGetTradingDays:
    """测试 get_trading_days"""
    
    def test_get_trading_days_in_range(self, mock_conn):
        """获取日期范围内的交易日"""
        conn, cursor = mock_conn
        cursor.description = [('date',), ('is_trading_day',)]
        cursor.fetchall.return_value = [
            ('2024-01-15', True),
            ('2024-01-16', True),
            ('2024-01-17', True)
        ]
        
        result = get_trading_days(conn, start_date='2024-01-15', end_date='2024-01-17')
        
        assert len(result) == 3
        sql = cursor.execute.call_args[0][0]
        assert 'BETWEEN' in sql
        assert 'is_trading_day = TRUE' in sql
    
    def test_get_trading_days_no_data(self, mock_conn):
        """范围内没有交易日返回空 DataFrame"""
        conn, cursor = mock_conn
        cursor.description = [('date',)]
        cursor.fetchall.return_value = []
        
        result = get_trading_days(conn, start_date='2024-01-01', end_date='2024-01-01')
        
        assert len(result) == 0
    
    def test_get_trading_days_different_market(self, mock_conn):
        """获取不同市场的交易日"""
        conn, cursor = mock_conn
        cursor.description = [('date',)]
        cursor.fetchall.return_value = []
        
        result = get_trading_days(
            conn, start_date='2024-01-15', end_date='2024-01-17', market='HK'
        )
        
        params = cursor.execute.call_args[0][1]
        assert 'HK' in params


class TestGetNextTradingDay:
    """测试 get_next_trading_day"""
    
    def test_get_next_trading_day_exists(self, mock_conn):
        """获取下一个交易日"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = ('2024-01-16',)
        
        result = get_next_trading_day(conn, date='2024-01-15')
        
        assert result == '2024-01-16'
        sql = cursor.execute.call_args[0][0]
        assert 'date >' in sql
        assert 'is_trading_day = TRUE' in sql
        assert 'LIMIT 1' in sql
    
    def test_get_next_trading_day_after_holiday(self, mock_conn):
        """节假日后的下一个交易日"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = ('2024-01-02',)
        
        result = get_next_trading_day(conn, date='2024-01-01')
        
        assert result == '2024-01-02'
    
    def test_get_next_trading_day_no_data(self, mock_conn):
        """没有更多交易日返回 None"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None
        
        result = get_next_trading_day(conn, date='2025-12-31')
        
        assert result is None


class TestGetPrevTradingDay:
    """测试 get_prev_trading_day"""
    
    def test_get_prev_trading_day_exists(self, mock_conn):
        """获取上一个交易日"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = ('2024-01-12',)
        
        result = get_prev_trading_day(conn, date='2024-01-15')
        
        assert result == '2024-01-12'
        sql = cursor.execute.call_args[0][0]
        assert 'date <' in sql
        assert 'ORDER BY date DESC' in sql
    
    def test_get_prev_trading_day_before_holiday(self, mock_conn):
        """节假日前的上一个交易日"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = ('2023-12-29',)
        
        result = get_prev_trading_day(conn, date='2024-01-01')
        
        assert result == '2023-12-29'
    
    def test_get_prev_trading_day_no_data(self, mock_conn):
        """没有更早的交易日返回 None"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None
        
        result = get_prev_trading_day(conn, date='1900-01-01')
        
        assert result is None
