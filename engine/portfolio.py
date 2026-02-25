# engine/portfolio.py

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict
import pandas as pd

from engine.constants import CASH_INSTRUMENT_ID


@dataclass
class Position:
    quantity: float
    buy_price: float


@dataclass
class Portfolio:

    cash: float

    slippage: float = 0.0
    transaction_cost: float = 0.0
    exchange_cost: float = 0.0
    reinvest_ratio: float = 1.0

    positions: Dict[int, Position] = field(default_factory=dict)

    # ============================================================
    # Helpers
    # ============================================================

    def total_value(self, prices: Dict[int, float]) -> float:
        total = self.cash
        for inst_id, pos in self.positions.items():
            if inst_id not in prices:
                # 停牌/退市：该持仓按 0 计入（rebalance 会清掉它）
                continue
            total += pos.quantity * prices[inst_id]
        return total

    # ============================================================
    # Rebalance (correct slippage)
    # ============================================================

    def rebalance(
        self,
        target_weights: Dict[int, float],
        prices: Dict[int, float],
    ):
        current_total = self.total_value(prices)
        target_total = current_total * self.reinvest_ratio

        new_positions: Dict[int, Position] = {}
        total_cost = 0.0

        for inst_id, weight in target_weights.items():

            if inst_id not in prices:
                raise KeyError(f"missing price for {inst_id}")

            price = prices[inst_id]
            if price <= 0:
                raise ValueError("price must be > 0")

            target_value = target_total * weight

            current_value = 0.0
            if inst_id in self.positions:
                current_value = self.positions[inst_id].quantity * price

            delta_value = target_value - current_value

            # 判断方向
            if delta_value > 0:
                exec_price = price * (1 + self.slippage)
            else:
                exec_price = price * (1 - self.slippage)

            # 计算交易成本（不包含滑点）
            trade_value = abs(delta_value)
            cost_rate = self.transaction_cost
            trade_cost = trade_value * cost_rate
            total_cost += trade_cost

            # 计算新数量
            quantity = target_value / exec_price

            new_positions[inst_id] = Position(
                quantity=quantity,
                buy_price=exec_price,
            )

        # 计算剩余现金
        invested_value = sum(
            pos.quantity * prices[i]
            for i, pos in new_positions.items()
        )

        remaining_cash = current_total - invested_value - total_cost

        self.positions = new_positions
        self.cash = remaining_cash

    # ============================================================
    # Snapshot
    # ============================================================

    def snapshot(self, date: str, prices: Dict[int, float]) -> pd.DataFrame:
        """
        生成持仓快照，包含所有股票持仓 + 现金
        现金使用 CASH_INSTRUMENT_ID (0) 作为占位符
        """
        rows = []

        # 股票持仓
        for inst_id, pos in self.positions.items():
            if inst_id not in prices:
                raise KeyError(f"missing price for {inst_id}")

            current_price = prices[inst_id]
            market_value = pos.quantity * current_price

            rows.append({
                "date": date,
                "instrument_id": inst_id,
                "quantity": pos.quantity,
                "buy_price": pos.buy_price,
                "current_price": current_price,
                "market_value": market_value,
            })

        # 现金持仓（使用特殊 ID=0）
        rows.append({
            "date": date,
            "instrument_id": CASH_INSTRUMENT_ID,
            "quantity": self.cash,
            "buy_price": 1.0,
            "current_price": 1.0,
            "market_value": self.cash,
        })

        return pd.DataFrame(rows)