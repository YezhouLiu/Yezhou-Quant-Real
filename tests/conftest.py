# =============================================================================
# Yezhou Capital Limited  |  Proprietary & Confidential
# =============================================================================
# Copyright (c) 2026 Yezhou Capital Limited. All rights reserved.
#
# Project  : Yezhou Quantitative Trading System
# Author   : Yezhou Liu
# Contact  : yezhoucapital@gmail.com
#
# This source code is the exclusive property of Yezhou Capital Limited.
# Unauthorized copying, modification, distribution, or use of this file,
# via any medium, is strictly prohibited without prior written consent.
# =============================================================================
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
