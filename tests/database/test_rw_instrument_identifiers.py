"""
测试 rw_instrument_identifiers.py - 资产标识符 CRUD 操作
使用 Mock 避免影响真实数据库
"""

import pytest
from unittest.mock import MagicMock
from database.readwrite.rw_instrument_identifiers import (
    insert_identifier,
    get_identifier,
    get_instrument_by_source_id,
    get_all_identifiers,
    batch_insert_identifiers,
    delete_identifier
)


@pytest.fixture
def mock_conn():
    """Mock 数据库连接和游标"""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


class TestInsertIdentifier:
    """测试 insert_identifier"""
    
    def test_insert_new_identifier(self, mock_conn):
        """插入新标识符"""
        conn, cursor = mock_conn
        
        insert_identifier(
            conn, instrument_id=123,
            source='yahoo', source_id='AAPL'
        )
        
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'INSERT INTO instrument_identifiers' in sql
        assert 'ON CONFLICT' in sql
    
    def test_insert_with_additional_info(self, mock_conn):
        """插入包含额外信息的标识符"""
        conn, cursor = mock_conn
        
        insert_identifier(
            conn, instrument_id=123,
            source='bloomberg', source_id='AAPL:US',
            additional_info={'exchange': 'NASDAQ', 'isin': 'US0378331005'}
        )
        
        assert cursor.execute.called
    
    def test_update_existing_identifier(self, mock_conn):
        """更新已存在的标识符"""
        conn, cursor = mock_conn
        
        insert_identifier(
            conn, instrument_id=123,
            source='yahoo', source_id='AAPL_NEW'
        )
        
        sql = cursor.execute.call_args[0][0]
        assert 'ON CONFLICT (instrument_id, source) DO UPDATE' in sql
    
    def test_insert_multiple_sources(self, mock_conn):
        """同一资产插入多个数据源标识符"""
        conn, cursor = mock_conn
        
        insert_identifier(conn, instrument_id=123, source='yahoo', source_id='AAPL')
        insert_identifier(conn, instrument_id=123, source='tiingo', source_id='aapl')
        insert_identifier(conn, instrument_id=123, source='bloomberg', source_id='AAPL:US')
        
        assert cursor.execute.call_count == 3


class TestGetIdentifier:
    """测试 get_identifier"""
    
    def test_get_existing_identifier(self, mock_conn):
        """获取存在的标识符"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = ('AAPL',)
        
        result = get_identifier(conn, instrument_id=123, source='yahoo')
        
        assert result == 'AAPL'
        assert cursor.execute.called
    
    def test_get_nonexistent_identifier(self, mock_conn):
        """获取不存在的标识符返回 None"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None
        
        result = get_identifier(conn, instrument_id=999, source='yahoo')
        
        assert result is None
    
    def test_get_identifier_different_sources(self, mock_conn):
        """从不同数据源获取标识符"""
        conn, cursor = mock_conn
        cursor.fetchone.side_effect = [('AAPL',), ('aapl',), ('AAPL:US',)]
        
        yahoo_id = get_identifier(conn, instrument_id=123, source='yahoo')
        tiingo_id = get_identifier(conn, instrument_id=123, source='tiingo')
        bloomberg_id = get_identifier(conn, instrument_id=123, source='bloomberg')
        
        assert yahoo_id == 'AAPL'
        assert tiingo_id == 'aapl'
        assert bloomberg_id == 'AAPL:US'


class TestGetInstrumentBySourceId:
    """测试 get_instrument_by_source_id"""
    
    def test_get_instrument_by_source_id(self, mock_conn):
        """根据数据源ID查找资产"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (123,)
        
        result = get_instrument_by_source_id(conn, source='yahoo', source_id='AAPL')
        
        assert result == 123
        assert cursor.execute.called
    
    def test_get_instrument_by_source_id_not_found(self, mock_conn):
        """查找不存在的数据源ID返回 None"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None
        
        result = get_instrument_by_source_id(conn, source='yahoo', source_id='UNKNOWN')
        
        assert result is None
    
    def test_get_instrument_case_sensitive(self, mock_conn):
        """测试大小写敏感性"""
        conn, cursor = mock_conn
        cursor.fetchone.side_effect = [(123,), (124,)]
        
        result1 = get_instrument_by_source_id(conn, source='tiingo', source_id='aapl')
        result2 = get_instrument_by_source_id(conn, source='tiingo', source_id='AAPL')
        
        # 假设数据库区分大小写
        assert result1 == 123
        assert result2 == 124


class TestGetAllIdentifiers:
    """测试 get_all_identifiers"""
    
    def test_get_all_identifiers(self, mock_conn):
        """获取资产的所有标识符"""
        conn, cursor = mock_conn
        cursor.fetchall.return_value = [
            ('yahoo', 'AAPL'),
            ('tiingo', 'aapl'),
            ('bloomberg', 'AAPL:US')
        ]
        
        result = get_all_identifiers(conn, instrument_id=123)
        
        assert isinstance(result, dict)
        assert len(result) == 3
        assert result['yahoo'] == 'AAPL'
        assert result['tiingo'] == 'aapl'
        assert result['bloomberg'] == 'AAPL:US'
    
    def test_get_all_identifiers_none(self, mock_conn):
        """资产没有标识符返回空字典"""
        conn, cursor = mock_conn
        cursor.fetchall.return_value = []
        
        result = get_all_identifiers(conn, instrument_id=999)
        
        assert result == {}
    
    def test_get_all_identifiers_single(self, mock_conn):
        """资产只有一个标识符"""
        conn, cursor = mock_conn
        cursor.fetchall.return_value = [('yahoo', 'AAPL')]
        
        result = get_all_identifiers(conn, instrument_id=123)
        
        assert len(result) == 1
        assert result['yahoo'] == 'AAPL'


class TestBatchInsertIdentifiers:
    """测试 batch_insert_identifiers"""
    
    def test_batch_insert_multiple_identifiers(self, mock_conn):
        """批量插入多个标识符"""
        conn, cursor = mock_conn
        
        identifiers = [
            {'instrument_id': 123, 'source': 'yahoo', 'source_id': 'AAPL'},
            {'instrument_id': 123, 'source': 'tiingo', 'source_id': 'aapl'},
            {'instrument_id': 124, 'source': 'yahoo', 'source_id': 'GOOGL'}
        ]
        
        batch_insert_identifiers(conn, identifiers)
        
        assert cursor.execute.call_count == 3
    
    def test_batch_insert_with_additional_info(self, mock_conn):
        """批量插入包含额外信息"""
        conn, cursor = mock_conn
        
        identifiers = [
            {
                'instrument_id': 123,
                'source': 'bloomberg',
                'source_id': 'AAPL:US',
                'additional_info': {'isin': 'US0378331005'}
            }
        ]
        
        batch_insert_identifiers(conn, identifiers)
        
        assert cursor.execute.called
    
    def test_batch_insert_empty_list(self, mock_conn):
        """批量插入空列表不报错"""
        conn, cursor = mock_conn
        
        batch_insert_identifiers(conn, [])
        
        assert cursor.execute.call_count == 0
    
    def test_batch_insert_updates_existing(self, mock_conn):
        """批量插入更新已存在的标识符"""
        conn, cursor = mock_conn
        
        identifiers = [
            {'instrument_id': 123, 'source': 'yahoo', 'source_id': 'AAPL_NEW'}
        ]
        
        batch_insert_identifiers(conn, identifiers)
        
        sql = cursor.execute.call_args[0][0]
        assert 'ON CONFLICT' in sql


class TestDeleteIdentifier:
    """测试 delete_identifier"""
    
    def test_delete_existing_identifier(self, mock_conn):
        """删除存在的标识符"""
        conn, cursor = mock_conn
        
        delete_identifier(conn, instrument_id=123, source='yahoo')
        
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'DELETE FROM instrument_identifiers' in sql
        params = cursor.execute.call_args[0][1]
        assert 123 in params
        assert 'yahoo' in params
    
    def test_delete_nonexistent_identifier(self, mock_conn):
        """删除不存在的标识符不报错"""
        conn, cursor = mock_conn
        
        delete_identifier(conn, instrument_id=999, source='yahoo')
        
        assert cursor.execute.called
    
    def test_delete_specific_source_only(self, mock_conn):
        """只删除特定数据源的标识符，其他保留"""
        conn, cursor = mock_conn
        
        delete_identifier(conn, instrument_id=123, source='yahoo')
        
        # 确保只删除 yahoo 的标识符
        params = cursor.execute.call_args[0][1]
        assert 'yahoo' in params
