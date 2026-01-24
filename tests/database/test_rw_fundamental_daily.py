"""
测试 rw_fundamental_daily.py - 每日基本面/估值数据 CRUD 操作
使用 Mock 避免影响真实数据库
"""

import pytest
from unittest.mock import MagicMock
from database.readwrite.rw_fundamental_daily import (
    insert_fundamental_daily,
    batch_insert_fundamental_daily,
    get_fundamental_daily,
    get_latest_fundamental_daily,
    delete_fundamental_daily,
)


@pytest.fixture
def mock_conn():
    """Mock 数据库连接和游标"""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


class TestInsertFundamentalDaily:
    """测试 insert_fundamental_daily"""

    def test_insert_fundamental_daily_basic(self, mock_conn):
        """插入单条每日数据"""
        conn, cursor = mock_conn

        insert_fundamental_daily(
            conn,
            instrument_id=123,
            metric_name="val.MarketCap",
            value=1_000_000_000.0,
            date="2024-01-15",
        )

        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert "INSERT INTO fundamental_daily" in sql
        assert "ON CONFLICT" in sql

        params = cursor.execute.call_args[0][1]
        assert 123 in params
        assert "2024-01-15" in params
        assert "val.MarketCap" in params

    def test_insert_fundamental_daily_with_currency(self, mock_conn):
        """插入包含 currency 的数据"""
        conn, cursor = mock_conn

        insert_fundamental_daily(
            conn,
            instrument_id=123,
            metric_name="val.PE_TTM",
            value=25.5,
            date="2024-01-15",
            currency="USD",
        )

        params = cursor.execute.call_args[0][1]
        assert "USD" in params

    def test_insert_fundamental_daily_with_source(self, mock_conn):
        """插入包含 data_source 的数据"""
        conn, cursor = mock_conn

        insert_fundamental_daily(
            conn,
            instrument_id=123,
            metric_name="val.PB",
            value=6.2,
            date="2024-01-15",
            source="tiingo",
        )

        params = cursor.execute.call_args[0][1]
        assert "tiingo" in params

    def test_update_existing_fundamental_daily(self, mock_conn):
        """更新已存在的每日数据（走 ON CONFLICT upsert）"""
        conn, cursor = mock_conn

        insert_fundamental_daily(
            conn,
            instrument_id=123,
            metric_name="val.PE_TTM",
            value=26.0,
            date="2024-01-15",
            source="tiingo",
        )

        sql = cursor.execute.call_args[0][0]
        assert "ON CONFLICT (instrument_id, date, metric_name)" in sql
        assert "DO UPDATE SET" in sql
        assert "metric_value = EXCLUDED.metric_value" in sql

    def test_insert_multiple_metrics_same_day(self, mock_conn):
        """同一天插入多个不同指标"""
        conn, cursor = mock_conn

        metrics = [
            ("val.MarketCap", 1_000_000_000.0),
            ("val.PE_TTM", 25.5),
            ("val.PB", 6.2),
            ("cap.SharesOutstanding", 15_000_000.0),
            ("cap.FloatShares", 12_000_000.0),
        ]

        for metric_name, value in metrics:
            insert_fundamental_daily(
                conn,
                instrument_id=123,
                metric_name=metric_name,
                value=value,
                date="2024-01-15",
            )

        assert cursor.execute.call_count == 5


class TestBatchInsertFundamentalDaily:
    """测试 batch_insert_fundamental_daily"""

    def test_batch_insert_multiple_records(self, mock_conn):
        """批量插入多条每日数据"""
        conn, cursor = mock_conn

        records = [
            {
                "instrument_id": 123,
                "date": "2024-01-15",
                "metric_name": "val.MarketCap",
                "value": 1_000_000_000.0,
            },
            {
                "instrument_id": 123,
                "date": "2024-01-15",
                "metric_name": "val.PE_TTM",
                "value": 25.5,
            },
            {
                "instrument_id": 123,
                "date": "2024-01-15",
                "metric_name": "val.PB",
                "value": 6.2,
            },
        ]

        batch_insert_fundamental_daily(conn, records)

        assert cursor.execute.call_count == 3
        sql = cursor.execute.call_args[0][0]
        assert "INSERT INTO fundamental_daily" in sql

    def test_batch_insert_with_sources_and_currency(self, mock_conn):
        """批量插入包含 currency / source"""
        conn, cursor = mock_conn

        records = [
            {
                "instrument_id": 123,
                "date": "2024-01-15",
                "metric_name": "val.MarketCap",
                "value": 1_000_000_000.0,
                "currency": "USD",
                "source": "tiingo",
            },
            {
                "instrument_id": 124,
                "date": "2024-01-15",
                "metric_name": "val.MarketCap",
                "value": 500_000_000.0,
                "currency": "USD",
                "source": "tiingo",
            },
        ]

        batch_insert_fundamental_daily(conn, records)

        assert cursor.execute.call_count == 2

        # 检查最后一次调用参数是否包含 USD/tiingo
        params = cursor.execute.call_args[0][1]
        assert "USD" in params
        assert "tiingo" in params

    def test_batch_insert_empty_list(self, mock_conn):
        """批量插入空列表不报错"""
        conn, cursor = mock_conn

        batch_insert_fundamental_daily(conn, [])

        assert cursor.execute.call_count == 0


class TestGetFundamentalDaily:
    """测试 get_fundamental_daily"""

    def test_get_all_daily(self, mock_conn):
        """获取某资产所有每日数据"""
        conn, cursor = mock_conn
        cursor.description = [
            ("instrument_id",),
            ("date",),
            ("metric_name",),
            ("metric_value",),
            ("currency",),
            ("data_source",),
        ]
        cursor.fetchall.return_value = [
            (123, "2024-01-15", "val.MarketCap", 1_000_000_000.0, "USD", "tiingo"),
            (123, "2024-01-15", "val.PE_TTM", 25.5, "USD", "tiingo"),
        ]

        result = get_fundamental_daily(conn, instrument_id=123)

        assert len(result) == 2
        sql = cursor.execute.call_args[0][0]
        assert "FROM fundamental_daily" in sql
        assert "ORDER BY date DESC" in sql

    def test_get_daily_by_metric(self, mock_conn):
        """获取某资产特定指标的每日数据"""
        conn, cursor = mock_conn
        cursor.description = [("date",), ("metric_value",)]
        cursor.fetchall.return_value = [
            ("2024-01-15", 25.5),
            ("2024-01-14", 25.1),
        ]

        result = get_fundamental_daily(conn, instrument_id=123, metric_name="val.PE_TTM")

        params = cursor.execute.call_args[0][1]
        assert "val.PE_TTM" in params

    def test_get_daily_date_range(self, mock_conn):
        """获取日期范围内的每日数据"""
        conn, cursor = mock_conn
        cursor.description = [("date",), ("metric_value",)]
        cursor.fetchall.return_value = [
            ("2024-01-15", 1_000_000_000.0),
        ]

        result = get_fundamental_daily(
            conn,
            instrument_id=123,
            start_date="2024-01-01",
            end_date="2024-01-15",
        )

        params = cursor.execute.call_args[0][1]
        assert "2024-01-01" in params
        assert "2024-01-15" in params

    def test_get_daily_combined_filters(self, mock_conn):
        """组合过滤条件：metric + start + end"""
        conn, cursor = mock_conn
        cursor.description = [("date",), ("metric_value",)]
        cursor.fetchall.return_value = [("2024-01-15", 6.2)]

        result = get_fundamental_daily(
            conn,
            instrument_id=123,
            metric_name="val.PB",
            start_date="2024-01-01",
            end_date="2024-01-15",
        )

        params = cursor.execute.call_args[0][1]
        assert "val.PB" in params
        assert "2024-01-01" in params
        assert "2024-01-15" in params

    def test_get_daily_no_data(self, mock_conn):
        """没有数据返回空 DataFrame"""
        conn, cursor = mock_conn
        cursor.description = [("metric_name",)]
        cursor.fetchall.return_value = []

        result = get_fundamental_daily(conn, instrument_id=999)

        assert len(result) == 0


class TestGetLatestFundamentalDaily:
    """测试 get_latest_fundamental_daily"""

    def test_get_latest_exists(self, mock_conn):
        """存在最新值"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (25.5,)

        result = get_latest_fundamental_daily(conn, instrument_id=123, metric_name="val.PE_TTM")

        assert result == 25.5
        sql = cursor.execute.call_args[0][0]
        assert "ORDER BY date DESC" in sql
        assert "LIMIT 1" in sql

    def test_get_latest_not_exists(self, mock_conn):
        """不存在返回 None"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None

        result = get_latest_fundamental_daily(conn, instrument_id=999, metric_name="val.PE_TTM")

        assert result is None

    def test_get_latest_multiple_metrics(self, mock_conn):
        """连续获取不同指标的最新值"""
        conn, cursor = mock_conn
        cursor.fetchone.side_effect = [
            (1_000_000_000.0,),  # MarketCap
            (25.5,),             # PE_TTM
            (6.2,),              # PB
        ]

        mcap = get_latest_fundamental_daily(conn, instrument_id=123, metric_name="val.MarketCap")
        pe = get_latest_fundamental_daily(conn, instrument_id=123, metric_name="val.PE_TTM")
        pb = get_latest_fundamental_daily(conn, instrument_id=123, metric_name="val.PB")

        assert mcap == 1_000_000_000.0
        assert pe == 25.5
        assert pb == 6.2


class TestDeleteFundamentalDaily:
    """测试 delete_fundamental_daily"""

    def test_delete_all_daily(self, mock_conn):
        """删除某资产所有每日数据"""
        conn, cursor = mock_conn

        delete_fundamental_daily(conn, instrument_id=123)

        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert "DELETE FROM fundamental_daily" in sql

    def test_delete_specific_metric(self, mock_conn):
        """只删除特定指标的每日数据"""
        conn, cursor = mock_conn

        delete_fundamental_daily(conn, instrument_id=123, metric_name="val.PE_TTM")

        params = cursor.execute.call_args[0][1]
        assert "val.PE_TTM" in params

    def test_delete_nonexistent(self, mock_conn):
        """删除不存在的数据不报错"""
        conn, cursor = mock_conn

        delete_fundamental_daily(conn, instrument_id=999)

        assert cursor.execute.called
