import pytest
from unittest.mock import MagicMock

import pandas as pd

from database.readwrite.rw_factor_values import (
    insert_factor_value,
    batch_insert_factor_values,
    get_factor_values,
    get_latest_factor_value,
    get_factor_snapshot,
    delete_factor_values,
)


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor

    cursor.__enter__.return_value = cursor
    cursor.__exit__.return_value = False

    return conn, cursor


def test_insert_factor_value_wraps_jsonb(mock_conn):
    conn, cursor = mock_conn

    insert_factor_value(
        conn,
        instrument_id=1,
        date="2026-01-01",
        factor_name="mom_21d",
        factor_value=0.123,
        factor_args={"lookback": 21, "skip": 0},
        config={"stage": "raw"},
        data_source="internal",
    )

    assert cursor.execute.called
    params = cursor.execute.call_args[0][1]

    # params: (..., factor_args, config, data_source)
    assert params[0] == 1
    assert params[1] == "2026-01-01"
    assert params[2] == "mom_21d"
    assert float(params[3]) == 0.123

    # Jsonb 类型断言（避免版本差异用类名判断）
    assert params[5].__class__.__name__ == "Jsonb"
    assert params[6].__class__.__name__ == "Jsonb"


def test_insert_factor_value_defaults_jsonb(mock_conn):
    conn, cursor = mock_conn

    insert_factor_value(
        conn,
        instrument_id=1,
        date="2026-01-01",
        factor_name="mom_21d",
        factor_value=0.1,
        factor_args=None,
        config=None,
    )

    params = cursor.execute.call_args[0][1]
    assert params[5].__class__.__name__ == "Jsonb"
    assert params[6].__class__.__name__ == "Jsonb"


def test_batch_insert_factor_values_empty_noop(mock_conn):
    conn, cursor = mock_conn

    batch_insert_factor_values(conn, [])
    assert not cursor.executemany.called


def test_batch_insert_factor_values_normalizes_and_wraps_jsonb(mock_conn):
    conn, cursor = mock_conn

    rows = [
        {
            "instrument_id": 1,
            "date": "2026-01-01",
            "factor_name": "mom_21d",
            "factor_value": 0.1,
            # 故意不给 factor_version / factor_args / config / data_source
        }
    ]

    batch_insert_factor_values(conn, rows)

    assert cursor.executemany.called
    passed_rows = cursor.executemany.call_args[0][1]
    assert len(passed_rows) == 1

    r0 = passed_rows[0]
    assert r0["factor_version"] == "v1"
    assert r0["data_source"] == "internal"
    assert r0["factor_args"].__class__.__name__ == "Jsonb"
    assert r0["config"].__class__.__name__ == "Jsonb"


def test_get_factor_values_builds_query_and_returns_df(mock_conn):
    conn, cursor = mock_conn

    cursor.description = [
        ("instrument_id",),
        ("date",),
        ("factor_name",),
        ("factor_value",),
    ]
    cursor.fetchall.return_value = [
        (1, "2026-01-01", "mom_21d", 0.1),
        (1, "2026-01-02", "mom_21d", 0.2),
    ]

    df = get_factor_values(
        conn,
        factor_name="mom_21d",
        factor_version="v1",
        instrument_id=1,
        start_date="2026-01-01",
        end_date="2026-01-31",
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["instrument_id", "date", "factor_name", "factor_value"]

    sql = cursor.execute.call_args[0][0]
    params = cursor.execute.call_args[0][1]
    assert "factor_name = %s" in sql
    assert "factor_version = %s" in sql
    assert "instrument_id = %s" in sql
    assert "date >= %s" in sql
    assert "date <= %s" in sql
    assert params == ["mom_21d", "v1", 1, "2026-01-01", "2026-01-31"]


def test_get_latest_factor_value_returns_value(mock_conn):
    conn, cursor = mock_conn
    cursor.fetchone.return_value = (0.123,)

    v = get_latest_factor_value(
        conn, instrument_id=1, factor_name="mom_21d", factor_version="v1"
    )
    assert v == 0.123


def test_get_latest_factor_value_returns_none(mock_conn):
    conn, cursor = mock_conn
    cursor.fetchone.return_value = None

    v = get_latest_factor_value(
        conn, instrument_id=1, factor_name="mom_21d", factor_version="v1"
    )
    assert v is None


def test_get_factor_snapshot_returns_df(mock_conn):
    conn, cursor = mock_conn
    cursor.description = [("instrument_id",), ("factor_value",)]
    cursor.fetchall.return_value = [(1, 0.1), (2, 0.2)]

    df = get_factor_snapshot(
        conn, factor_name="mom_21d", date="2026-01-02", factor_version="v1"
    )
    assert len(df) == 2
    assert list(df.columns) == ["instrument_id", "factor_value"]


def test_delete_factor_values_builds_query(mock_conn):
    conn, cursor = mock_conn

    delete_factor_values(conn, factor_name="mom_21d", factor_version="v1")

    sql = cursor.execute.call_args[0][0]
    params = cursor.execute.call_args[0][1]
    assert "DELETE FROM factor_values" in sql
    assert "factor_name = %s" in sql
    assert "factor_version = %s" in sql
    assert params == ["mom_21d", "v1"]


def test_get_factor_values_with_factor_names_list(mock_conn):
    """测试多因子名查询"""
    conn, cursor = mock_conn
    cursor.description = [
        ("instrument_id",),
        ("date",),
        ("factor_name",),
        ("factor_value",),
    ]
    cursor.fetchall.return_value = [
        (1, "2026-01-01", "mom_21d", 0.1),
        (1, "2026-01-01", "vol_20d", 0.2),
    ]

    df = get_factor_values(
        conn,
        factor_names=["mom_21d", "vol_20d"],
        date="2026-01-01",
    )

    assert len(df) == 2
    sql = cursor.execute.call_args[0][0]
    params = cursor.execute.call_args[0][1]
    assert "factor_name = ANY(%s)" in sql
    assert params[0] == ["mom_21d", "vol_20d"]
    assert params[1] == "2026-01-01"


def test_get_factor_values_with_instrument_ids_list(mock_conn):
    """测试多标的 ID 查询"""
    conn, cursor = mock_conn
    cursor.description = [
        ("instrument_id",),
        ("date",),
        ("factor_name",),
        ("factor_value",),
    ]
    cursor.fetchall.return_value = [
        (1, "2026-01-01", "mom_21d", 0.1),
        (2, "2026-01-01", "mom_21d", 0.2),
    ]

    df = get_factor_values(
        conn,
        factor_name="mom_21d",
        instrument_ids=[1, 2],
        date="2026-01-01",
    )

    assert len(df) == 2
    sql = cursor.execute.call_args[0][0]
    params = cursor.execute.call_args[0][1]
    assert "instrument_id = ANY(%s)" in sql
    assert [1, 2] in params


def test_get_factor_values_mutual_exclusion_factor_name(mock_conn):
    """测试 factor_name 和 factor_names 互斥"""
    conn, cursor = mock_conn

    with pytest.raises(ValueError, match="mutually exclusive"):
        get_factor_values(
            conn,
            factor_name="mom_21d",
            factor_names=["vol_20d"],
        )


def test_get_factor_values_mutual_exclusion_instrument_id(mock_conn):
    """测试 instrument_id 和 instrument_ids 互斥"""
    conn, cursor = mock_conn

    with pytest.raises(ValueError, match="mutually exclusive"):
        get_factor_values(
            conn,
            instrument_id=1,
            instrument_ids=[1, 2],
        )


def test_get_factor_values_mutual_exclusion_date(mock_conn):
    """测试 date 与 start_date/end_date 互斥"""
    conn, cursor = mock_conn

    with pytest.raises(ValueError, match="mutually exclusive"):
        get_factor_values(
            conn,
            date="2026-01-01",
            start_date="2026-01-01",
        )


def test_get_factor_values_empty_lists_raise(mock_conn):
    """测试空列表报错"""
    conn, cursor = mock_conn

    with pytest.raises(ValueError, match="cannot be empty"):
        get_factor_values(conn, factor_names=[])

    with pytest.raises(ValueError, match="cannot be empty"):
        get_factor_values(conn, instrument_ids=[])

