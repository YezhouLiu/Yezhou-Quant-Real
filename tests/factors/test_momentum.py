import datetime as dt
import pandas as pd
import pytest
from unittest.mock import MagicMock

from factors.momentum import calc_single_instrument_momentum


@pytest.fixture
def mock_conn():
    return MagicMock()


class CaptureInsert:
    def __init__(self):
        self.rows = None

    def __call__(self, conn, rows):
        self.rows = rows


def test_roll_start_has_no_value_but_later_has_values(monkeypatch, mock_conn):
    """
    关键语义：
    - start_date 很早（比如 2005-01-01），前 lookback 天没有 momentum 很正常
    - 但到了后面（比如 2005 年后很久/2016），必须能写出 momentum
    """
    cap = CaptureInsert()
    monkeypatch.setattr("factors.momentum.batch_insert_factor_values", cap)

    # 构造一条足够长的日序列（用自然日模拟，够长即可）
    start = dt.date(2004, 1, 1)   # 提供 start_date 之前的历史
    end = dt.date(2017, 1, 10)
    dates = pd.date_range(start, end, freq="D")

    # 单调递增价格：保证 momentum 合理且稳定
    df = pd.DataFrame(
        {
            "instrument_id": 1,
            "date": dates,
            "adj_close": [100.0 + i * 0.01 for i in range(len(dates))],
        }
    )

    def fake_get_prices(conn, instrument_id, start_date=None, end_date=None):
        # 模拟 SQL 过滤效果
        out = df.copy()
        if start_date:
            out = out[out["date"] >= pd.to_datetime(start_date)]
        if end_date:
            out = out[out["date"] <= pd.to_datetime(end_date)]
        return out.reset_index(drop=True)

    monkeypatch.setattr("factors.momentum.get_prices", fake_get_prices)

    n = calc_single_instrument_momentum(
        mock_conn,
        instrument_id=1,
        start_date="2005-01-01",
        end_date="2016-01-01",
        lookback=252,
        skip=21,
        factor_version="v1",
    )

    assert n > 0
    assert cap.rows is not None
    # 写入日期必须落在写表区间内
    assert min(r["date"] for r in cap.rows) >= "2005-01-01"
    assert max(r["date"] for r in cap.rows) <= "2016-01-01"


def test_buffer_is_required(monkeypatch, mock_conn):
    """
    如果不取 start_date 之前的历史，rolling 因子会被截断。
    这个 test 用来确保函数确实在取价时向前扩展了窗口（通过断言 fake_get_prices 收到的 start_date）。
    """
    cap = CaptureInsert()
    monkeypatch.setattr("factors.momentum.batch_insert_factor_values", cap)

    received = {"start_date": None, "end_date": None}

    # 返回一段很长的价格（足够算出来）
    dates = pd.date_range(dt.date(2004, 1, 1), dt.date(2006, 12, 31), freq="D")
    df = pd.DataFrame(
        {"instrument_id": 1, "date": dates, "adj_close": [100.0 + i * 0.01 for i in range(len(dates))]}
    )

    def fake_get_prices(conn, instrument_id, start_date=None, end_date=None):
        received["start_date"] = start_date
        received["end_date"] = end_date
        out = df.copy()
        if start_date:
            out = out[out["date"] >= pd.to_datetime(start_date)]
        if end_date:
            out = out[out["date"] <= pd.to_datetime(end_date)]
        return out.reset_index(drop=True)

    monkeypatch.setattr("factors.momentum.get_prices", fake_get_prices)

    _ = calc_single_instrument_momentum(
        mock_conn,
        instrument_id=1,
        start_date="2005-01-01",
        end_date="2006-01-01",
        lookback=252,
        skip=21,
    )

    assert received["start_date"] is not None
    # 必须早于写表 start_date，说明函数确实取了 buffer
    assert pd.to_datetime(received["start_date"]).date() < dt.date(2005, 1, 1)

def test_empty_prices_returns_0(monkeypatch, mock_conn):
    monkeypatch.setattr("factors.momentum.get_prices", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr("factors.momentum.batch_insert_factor_values", lambda *args, **kwargs: None)

    n = calc_single_instrument_momentum(
        mock_conn,
        instrument_id=1,
        start_date="2005-01-01",
        end_date="2006-01-01",
        lookback=252,
        skip=21,
    )
    assert n == 0
