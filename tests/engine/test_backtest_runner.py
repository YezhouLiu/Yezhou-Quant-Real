import pytest
from unittest.mock import MagicMock
import pandas as pd

from engine.backtest_runner import BacktestRunner
from engine.constants import CASH_INSTRUMENT_ID


class DummySelector:
    def select(self, signals):
        return type(
            "Sel", (), {"selected": signals[["instrument_id"]], "ranking_col": None}
        )


class DummyStrategy:
    def score_for_date(self, conn, asof_date, universe_ids):
        return type(
            "Res",
            (),
            {
                "signals": pd.DataFrame({"instrument_id": universe_ids}),
                "score_col": None,
            },
        )


def test_runner_writes_exp_positions(monkeypatch):

    conn = MagicMock()

    # 模拟 get_trading_days 返回一个只有 "2024-01-31" 的 DataFrame
    monkeypatch.setattr(
        "engine.backtest_runner.get_trading_days",
        lambda conn, start, end, market="US": pd.DataFrame({"date": ["2024-01-31"]}),
    )

    monkeypatch.setattr(
        "engine.backtest_runner.get_prices_on_date",
        lambda conn, ids, date, strict=False: {i: 100.0 for i in ids},
    )

    called = []

    def fake_batch(conn, rows):
        called.append(len(rows))

    monkeypatch.setattr("engine.backtest_runner.batch_insert_exp_positions", fake_batch)

    monkeypatch.setattr(
        "engine.backtest_runner.delete_exp_positions_by_date", lambda conn, date: None
    )

    runner = BacktestRunner(
        strategy=DummyStrategy(),
        selector=DummySelector(),
        initial_cash=100000,
        slippage=0.0,
        transaction_cost=0.0,
        exchange_cost=0.0,
        reinvest_ratio=1.0,
        universe_provider=lambda d: [1, 2],
        rebalance_day="last",   # 每月最后一个交易日
        market="US",
    )

    runner.run(conn, start_date="2024-01-01", end_date="2024-01-31")

    assert len(called) == 1
    assert called[0] >= 1
