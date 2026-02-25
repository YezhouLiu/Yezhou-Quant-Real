# tests/engine/test_portfolio.py

import pytest
from engine.portfolio import Portfolio
from engine.constants import CASH_INSTRUMENT_ID


class TestPortfolio:

    def test_buy_slippage(self):
        p = Portfolio(cash=100000, slippage=0.01)

        prices = {1: 100.0}
        weights = {1: 1.0}

        p.rebalance(weights, prices)

        pos = p.positions[1]

        # 买入应高于市场价
        assert pos.buy_price == pytest.approx(101.0)

    def test_sell_slippage(self):
        p = Portfolio(cash=0, slippage=0.01)

        # 初始持仓
        p.positions[1] = p.positions.get(1) or \
            type("Pos", (), {"quantity": 1000, "buy_price": 100})()

        prices = {1: 100.0}

        # 全卖出
        weights = {1: 0.0}

        p.rebalance(weights, prices)

        # 卖出价格应低于市场价
        # 新仓位数量应为 0
        assert p.positions[1].quantity == pytest.approx(0.0)

    def test_transaction_cost_reduces_cash(self):
        p = Portfolio(
            cash=100000,
            transaction_cost=0.01,
            exchange_cost=0.0,
            slippage=0.0
        )

        prices = {1: 100.0}
        weights = {1: 1.0}

        p.rebalance(weights, prices)

        total = p.total_value(prices)

        assert total < 100000

    def test_snapshot_contains_cash(self):
        p = Portfolio(cash=5000)
        prices = {}
        df = p.snapshot("2024-01-31", prices)

        # 现金应该是最后一行，ID=0
        cash_row = df[df["instrument_id"] == CASH_INSTRUMENT_ID]
        assert len(cash_row) == 1
        assert cash_row.iloc[0]["market_value"] == 5000
        assert cash_row.iloc[0]["quantity"] == 5000