from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Callable, List, Dict
import psycopg

from engine.portfolio import Portfolio
from engine.strategies.scoring_strategy import ScoringStrategy
from engine.selectors.base import Selector
from engine.constants import CASH_INSTRUMENT_ID

from database.readwrite.rw_market_prices import get_prices_on_date
from database.readwrite.rw_trading_calendar import get_trading_days
from database.readwrite.rw_exp_positions import (
    batch_insert_exp_positions,
    delete_exp_positions_by_date,
)


@dataclass
class BacktestRunner:
    """
    回测运行器

    注意：现金使用 CASH_INSTRUMENT_ID (0) 表示，无需配置

    rebalance_day: 每月调仓在哪一天触发
      - 'last'  : 每月最后一个交易日（默认）
      - 'first' : 每月第一个交易日
      - 1~28    : 每月第 N 个自然日 -> 若当日非交易日，顺延至下一个交易日
    """

    strategy: ScoringStrategy
    selector: Selector

    initial_cash: float

    slippage: float
    transaction_cost: float
    exchange_cost: float
    reinvest_ratio: float

    universe_provider: Callable[[str], Sequence[int]]
    rebalance_day: str | int = "last"  # 'last' | 'first' | 1~28
    market: str = "US"

    # ============================================================
    # Main
    # ============================================================

    def run(
        self,
        conn: psycopg.Connection,
        *,
        start_date: str,
        end_date: str,
        overwrite: bool = True,
    ):
        # 从 trading_calendar 直接读取交易日
        df_cal = get_trading_days(conn, start_date, end_date, market=self.market)
        trading_days = df_cal["date"].astype(str).tolist()

        portfolio = Portfolio(
            cash=self.initial_cash,
            slippage=self.slippage,
            transaction_cost=self.transaction_cost,
            exchange_cost=self.exchange_cost,
            reinvest_ratio=self.reinvest_ratio,
        )

        rebalance_dates = self._generate_rebalance_dates(conn, trading_days)

        for date in trading_days:

            # 只处理调仓日
            if date not in rebalance_dates:
                continue

            if overwrite:
                delete_exp_positions_by_date(conn, date)

            # ==========================
            # Rebalance
            # ==========================
            universe_ids = self.universe_provider(date)

            try:
                score_result = self.strategy.score_for_date(
                    conn,
                    asof_date=date,
                    universe_ids=universe_ids,
                )
            except (KeyError, ValueError) as e:
                # 该日期没有因子数据，跳过 rebalance
                print(f"[WARN] Skipping rebalance on {date}: {e}")
                continue

            selection = self.selector.select(score_result.signals)

            selected_ids = selection.selected["instrument_id"].tolist()

            # 需要价格的标的 = 新选标的 + 当前持仓（rebalance 时计算 total_value 需要）
            all_needed_ids = list(set(selected_ids) | set(portfolio.positions.keys()))

            # 获取价格（非严格模式：自动过滤缺失价格的标的）
            prices = get_prices_on_date(conn, all_needed_ids, date, strict=False)

            # 只对有价格的标的进行配权
            valid_ids = [i for i in selected_ids if i in prices]

            if not valid_ids:
                continue

            weights = self._equal_weight(valid_ids)
            portfolio.rebalance(weights, prices)

            # ==========================
            # 调仓日快照
            # ==========================
            df_snapshot = portfolio.snapshot(
                date=date,
                prices=prices,
            )

            batch_insert_exp_positions(
                conn,
                df_snapshot.to_dict("records"),
            )

    # ============================================================
    # Helpers
    # ============================================================

    def _equal_weight(self, ids: Sequence[int]) -> Dict[int, float]:
        if not ids:
            raise ValueError("no selected ids")
        w = 1.0 / len(ids)
        return {i: w for i in ids}

    def _generate_rebalance_dates(
        self,
        conn: psycopg.Connection,
        trading_days: List[str],
    ) -> set[str]:
        """
        根据 rebalance_day 生成每月调仓日集合，所有日期均保证是真实交易日。

        - 'last'  : 每月最后一个交易日
        - 'first' : 每月第一个交易日
        - 1~28    : 每月第 N 自然日；若该日非交易日，取其之后第一个交易日
        """
        import pandas as pd
        from database.readwrite.rw_trading_calendar import get_next_trading_day

        df = pd.DataFrame({"date": pd.to_datetime(trading_days)})
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month
        df["date_str"] = df["date"].dt.strftime("%Y-%m-%d")

        rebalance: set[str] = set()

        if self.rebalance_day == "last":
            dates = (
                df.sort_values("date")
                .groupby(["year", "month"])["date_str"]
                .last()
                .tolist()
            )
            rebalance = set(dates)

        elif self.rebalance_day == "first":
            dates = (
                df.sort_values("date")
                .groupby(["year", "month"])["date_str"]
                .first()
                .tolist()
            )
            rebalance = set(dates)

        elif isinstance(self.rebalance_day, int):
            if not (1 <= self.rebalance_day <= 28):
                raise ValueError("rebalance_day as int must be between 1 and 28")

            trading_day_set = set(trading_days)

            for (year, month), _ in df.groupby(["year", "month"]):
                target = f"{year}-{month:02d}-{self.rebalance_day:02d}"

                if target in trading_day_set:
                    rebalance.add(target)
                else:
                    # 顺延至下一个交易日
                    next_td = get_next_trading_day(conn, target, market=self.market)
                    if next_td is not None:
                        next_str = str(next_td)
                        # 只加入本月的交易日
                        if next_str in trading_day_set:
                            rebalance.add(next_str)
        else:
            raise ValueError(
                f"rebalance_day must be 'last', 'first', or an int 1-28, got {self.rebalance_day!r}"
            )

        return rebalance
