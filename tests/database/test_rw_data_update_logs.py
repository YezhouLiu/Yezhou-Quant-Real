"""
测试 rw_data_update_logs.py - 数据更新日志 CRUD 操作
使用 Mock 避免影响真实数据库
"""

import pytest
from unittest.mock import MagicMock
from database.readwrite.rw_data_update_logs import (
    create_log,
    update_log_success,
    update_log_failure,
    get_recent_logs
)


@pytest.fixture
def mock_conn():
    """Mock 数据库连接和游标"""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


class TestCreateLog:
    """测试 create_log"""
    
    def test_create_log_minimal(self, mock_conn):
        """创建最小日志记录"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (1,)
        
        result = create_log(conn, dataset='market_prices', source='tiingo')
        
        assert result == 1
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'INSERT INTO data_update_logs' in sql
        assert "'running'" in sql
    
    def test_create_log_with_date_range(self, mock_conn):
        """创建包含日期范围的日志"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (2,)
        
        result = create_log(
            conn, dataset='market_prices', source='tiingo',
            start_date='2024-01-01', end_date='2024-01-31'
        )
        
        assert result == 2
        params = cursor.execute.call_args[0][1]
        assert '2024-01-01' in params
        assert '2024-01-31' in params
    
    def test_create_log_with_instrument_count(self, mock_conn):
        """创建包含资产数量的日志"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (3,)
        
        result = create_log(
            conn, dataset='market_prices', source='tiingo',
            instruments_count=500
        )
        
        assert result == 3
        params = cursor.execute.call_args[0][1]
        assert 500 in params
    
    def test_create_log_different_datasets(self, mock_conn):
        """创建不同数据集的日志"""
        conn, cursor = mock_conn
        cursor.fetchone.side_effect = [(1,), (2,), (3,)]
        
        log1 = create_log(conn, dataset='market_prices', source='tiingo')
        log2 = create_log(conn, dataset='fundamentals', source='yahoo')
        log3 = create_log(conn, dataset='universe', source='csv')
        
        assert log1 == 1
        assert log2 == 2
        assert log3 == 3


class TestUpdateLogSuccess:
    """测试 update_log_success"""
    
    def test_update_log_success_basic(self, mock_conn):
        """更新日志为成功状态"""
        conn, cursor = mock_conn
        
        update_log_success(conn, log_id=1)
        
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'UPDATE data_update_logs' in sql
        assert "status = 'completed'" in sql
        assert 'completed_at = now()' in sql
    
    def test_update_log_success_with_counts(self, mock_conn):
        """更新日志并记录插入/更新数量"""
        conn, cursor = mock_conn
        
        update_log_success(conn, log_id=1, rows_inserted=1000, rows_updated=50)
        
        params = cursor.execute.call_args[0][1]
        assert 1000 in params
        assert 50 in params
    
    def test_update_log_success_calculates_duration(self, mock_conn):
        """更新日志时计算执行时长"""
        conn, cursor = mock_conn
        
        update_log_success(conn, log_id=1)
        
        sql = cursor.execute.call_args[0][0]
        assert 'duration_seconds' in sql
        assert 'EXTRACT(EPOCH FROM' in sql
    
    def test_update_log_success_zero_rows(self, mock_conn):
        """更新日志时没有插入/更新数据"""
        conn, cursor = mock_conn
        
        update_log_success(conn, log_id=1, rows_inserted=0, rows_updated=0)
        
        assert cursor.execute.called


class TestUpdateLogFailure:
    """测试 update_log_failure"""
    
    def test_update_log_failure_basic(self, mock_conn):
        """更新日志为失败状态"""
        conn, cursor = mock_conn
        
        update_log_failure(conn, log_id=1, error_message='Connection timeout')
        
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'UPDATE data_update_logs' in sql
        assert "status = 'failed'" in sql
        params = cursor.execute.call_args[0][1]
        assert 'Connection timeout' in params
    
    def test_update_log_failure_with_detailed_error(self, mock_conn):
        """更新日志并记录详细错误信息"""
        conn, cursor = mock_conn
        
        error_msg = 'API rate limit exceeded: 429 Too Many Requests'
        update_log_failure(conn, log_id=1, error_message=error_msg)
        
        params = cursor.execute.call_args[0][1]
        assert error_msg in params
    
    def test_update_log_failure_calculates_duration(self, mock_conn):
        """失败日志也计算执行时长"""
        conn, cursor = mock_conn
        
        update_log_failure(conn, log_id=1, error_message='Error')
        
        sql = cursor.execute.call_args[0][0]
        assert 'duration_seconds' in sql
    
    def test_update_log_failure_different_errors(self, mock_conn):
        """记录不同类型的错误"""
        conn, cursor = mock_conn
        
        update_log_failure(conn, log_id=1, error_message='Network error')
        update_log_failure(conn, log_id=2, error_message='Invalid API key')
        update_log_failure(conn, log_id=3, error_message='Data format error')
        
        assert cursor.execute.call_count == 3


class TestGetRecentLogs:
    """测试 get_recent_logs"""
    
    def test_get_recent_logs_default(self, mock_conn):
        """获取最近的日志（默认10条）"""
        conn, cursor = mock_conn
        cursor.description = [
            ('log_id',), ('dataset',), ('status',), ('started_at',)
        ]
        cursor.fetchall.return_value = [
            (1, 'market_prices', 'completed', '2024-01-15 09:00:00'),
            (2, 'fundamentals', 'completed', '2024-01-15 10:00:00')
        ]
        
        result = get_recent_logs(conn)
        
        assert len(result) == 2
        sql = cursor.execute.call_args[0][0]
        assert 'ORDER BY started_at DESC' in sql
        assert 'LIMIT' in sql
    
    def test_get_recent_logs_by_dataset(self, mock_conn):
        """获取特定数据集的最近日志"""
        conn, cursor = mock_conn
        cursor.description = [('log_id',), ('dataset',)]
        cursor.fetchall.return_value = [(1, 'market_prices'), (2, 'market_prices')]
        
        result = get_recent_logs(conn, dataset='market_prices')
        
        params = cursor.execute.call_args[0][1]
        assert 'market_prices' in params
    
    def test_get_recent_logs_custom_limit(self, mock_conn):
        """获取自定义数量的日志"""
        conn, cursor = mock_conn
        cursor.description = [('log_id',)]
        cursor.fetchall.return_value = [(i,) for i in range(1, 21)]
        
        result = get_recent_logs(conn, limit=20)
        
        params = cursor.execute.call_args[0][1]
        assert 20 in params
    
    def test_get_recent_logs_no_data(self, mock_conn):
        """没有日志时返回空 DataFrame"""
        conn, cursor = mock_conn
        cursor.description = [('log_id',)]
        cursor.fetchall.return_value = []
        
        result = get_recent_logs(conn)
        
        assert len(result) == 0
    
    def test_get_recent_logs_filters_and_limits(self, mock_conn):
        """同时使用数据集过滤和数量限制"""
        conn, cursor = mock_conn
        cursor.description = [('log_id',), ('dataset',)]
        cursor.fetchall.return_value = [(1, 'market_prices')]
        
        result = get_recent_logs(conn, dataset='market_prices', limit=5)
        
        params = cursor.execute.call_args[0][1]
        assert 'market_prices' in params
        assert 5 in params
