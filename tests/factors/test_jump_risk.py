import datetime as dt
import pandas as pd
import pytest
from unittest.mock import MagicMock

from factors.jump_risk import calc_single_instrument_jump_risk


@pytest.fixture
def mock_conn():
    return MagicMock()


class CaptureInsert:
    def __init__(self):
        self.rows = None

    def __call__(self, conn, rows):
        self.rows = rows


def test_jump_detected_and_aggregated(monkeypatch, mock_conn):
    cap = CaptureInsert()
    monkeypatch.setattr("factors.jump_risk.batch_insert_factor_values", cap)

    dates = pd.date_range(dt.date(2020, 1, 1), dt.date(2020, 6, 30), freq="D")
    prices = [100.0] * len(dates)
    prices[50] = 200.0   # big jump
    prices[120] = 50.0   # another jump

    df = pd.DataFrame(
        {"instrument_id": 1, "date": dates, "adj_close": prices}
    )

    def fake_get_prices(conn, instrument_id, start_date=None, end_date=None):
        out = df.copy()
        if start_date:
            out = out[out["date"] >= pd.to_datetime(start_date)]
        if end_date:
            out = out[out["date"] <= pd.to_datetime(end_date)]
        return out.reset_index(drop=True)

    monkeypatch.setattr("factors.jump_risk.get_prices", fake_get_prices)

    n = calc_single_instrument_jump_risk(
        mock_conn,
        instrument_id=1,
        start_date="2020-02-01",
        end_date="2020-06-01",
        window=60,
        jump_threshold=0.5,
        factor_version="v1",
    )

    assert n > 0
    assert cap.rows is not None


def test_empty_prices_returns_0(monkeypatch, mock_conn):
    monkeypatch.setattr("factors.jump_risk.get_prices", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr("factors.jump_risk.batch_insert_factor_values", lambda *args, **kwargs: None)

    n = calc_single_instrument_jump_risk(
        mock_conn,
        instrument_id=1,
        start_date="2020-01-01",
        end_date="2020-02-01",
    )
    assert n == 0
