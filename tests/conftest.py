"""
Pytest 配置文件 - 全局 fixtures 和配置
"""

import pytest
from unittest.mock import MagicMock


@pytest.fixture
def mock_db_connection():
    """
    通用的数据库连接 Mock fixture
    可在所有测试中使用
    """
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=None)
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=None)
    
    return conn, cursor


def pytest_configure(config):
    """Pytest 配置钩子"""
    config.addinivalue_line(
        "markers", "database: 标记数据库相关测试"
    )
    config.addinivalue_line(
        "markers", "unit: 标记单元测试"
    )
    config.addinivalue_line(
        "markers", "integration: 标记集成测试"
    )
