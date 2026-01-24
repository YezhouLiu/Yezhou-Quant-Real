import pytest
from unittest.mock import MagicMock, patch

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# 待测函数
from data_download.update.update_tradable_universe import update_tradable_universe

@pytest.fixture
def mock_conn_cursor():
    """
    Mock 一个 conn + cursor
    """
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


@patch("tools.update_tradable_universe.get_db_connection")
def test_update_tradable_universe_filters_stocks(mock_get_conn, mock_conn_cursor):
    """
    核心测试：
    - 使用 market_prices
    - 会执行多次 UPDATE
    - 会实际过滤 Stock
    """

    conn, cursor = mock_conn_cursor
    mock_get_conn.return_value = conn

    # -----------------------------
    # mock 返回值顺序非常关键
    # -----------------------------
    # 1) SELECT MAX(date) FROM market_prices
    cursor.fetchone.side_effect = [
        ("2026-01-24",),  # last_px_date
    ]

    # 2) summary SELECT asset_type, COUNT(*)
    cursor.fetchall.return_value = [
        ("ETF", 120),
        ("Stock", 850),
    ]

    # 模拟 UPDATE 影响行数（这是“是否真的在滤”的关键）
    cursor.rowcount = 123

    # -----------------------------
    # 执行
    # -----------------------------
    update_tradable_universe(
        tradable_only=False,
        min_price=3.0,
        min_avg_volume=300_000,
        volume_lookback_days=20,
        commit=True,
    )

    # -----------------------------
    # 断言：是否真的跑了核心 SQL
    # -----------------------------
    executed_sql = " ".join(
        call.args[0] for call in cursor.execute.call_args_list
    )

    # ✅ 核心表名必须出现
    assert "market_prices" in executed_sql
    assert "instruments" in executed_sql

    # ❌ 不允许再出现旧表
    assert "price_history" not in executed_sql

    # ✅ 必须有 is_tradable = FALSE（说明真的在“剔除”）
    assert "SET is_tradable = FALSE" in executed_sql

    # ✅ ETF 放行
    assert "asset_type = 'ETF'" in executed_sql

    # -----------------------------
    # commit 被调用
    # -----------------------------
    conn.commit.assert_called_once()
    conn.close.assert_called_once()


@patch("tools.update_tradable_universe.get_db_connection")
def test_update_tradable_universe_dry_run(mock_get_conn, mock_conn_cursor):
    """
    commit=False 时应 rollback
    """
    conn, cursor = mock_conn_cursor
    mock_get_conn.return_value = conn

    cursor.fetchone.side_effect = [
        ("2026-01-24",),
    ]
    cursor.fetchall.return_value = []

    update_tradable_universe(commit=False)

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    conn.close.assert_called_once()


@patch("tools.update_tradable_universe.get_db_connection")
def test_fail_when_market_prices_empty(mock_get_conn, mock_conn_cursor):
    """
    market_prices 为空时应直接失败（这是你要求的：早点炸）
    """
    conn, cursor = mock_conn_cursor
    mock_get_conn.return_value = conn

    cursor.fetchone.return_value = (None,)  # MAX(date) is None

    with pytest.raises(ValueError):
        update_tradable_universe()

    conn.close.assert_called_once()
