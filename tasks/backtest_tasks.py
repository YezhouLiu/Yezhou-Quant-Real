# backtest_tasks.py

from database.utils.db_utils import get_db_connection

from engine.signals import FactorSpec
from engine.scorers.linear import LinearScorer, LinearTerm
from engine.strategies.scoring_strategy import ScoringStrategy
from engine.selectors.topk import TopKSelector
from engine.backtest_runner import BacktestRunner
from engine.constants import CASH_INSTRUMENT_ID


# ============================================================
# Helpers
# ============================================================


def universe_all_tradable(conn, date: str):
    """
    简单 universe：当日 is_tradable = true
    
    注意：返回 None 表示不限制 universe（使用数据库中所有有因子数据的标的）
    这样可以避免传递大列表导致查询变慢
    """
    # 不返回具体列表，让因子查询自动筛选
    return None


# ============================================================
# Main Backtest
# ============================================================


def run_backtest():

    conn = get_db_connection()

    # ===============================
    # 1️⃣ 定义因子规格
    # ===============================

    specs = (
        # 动量因子（越大越好）
        FactorSpec(
            factor_name="mom_63d",
            ascending=True,
            methods=("rank",),
        ),
        # 波动率因子（越小越好 - 低波动）
        FactorSpec(
            factor_name="vol_60d_ann252",
            ascending=False,
            methods=("rank",),
        ),
        # 最大回撤因子（越小越好 - 风险控制）
        FactorSpec(
            factor_name="mdd_252d",
            ascending=False,
            methods=("rank",),
        ),
    )

    # ===============================
    # 2️⃣ 定义线性打分
    # ===============================

    scorer = LinearScorer(
        terms=(
            LinearTerm("mom_63d_rank", 0.5),        # 50% 权重：动量
            LinearTerm("vol_60d_ann252_rank", 0.3), # 30% 权重：低波动
            LinearTerm("mdd_252d_rank", 0.2),       # 20% 权重：低回撤
        ),
        out_col="_score",
        post_transform=None,
    )

    strategy = ScoringStrategy(
        factor_specs=specs,
        scorer=scorer,
        factor_version="v1",  # 匹配数据库中的版本
    )

    # ===============================
    # 3️⃣ TopK 选择
    # ===============================

    selector = TopKSelector(
        k=5,
        sort_by="_score",
        sort_ascending=False,
    )

    # ===============================
    # 4️⃣ Backtest Runner
    # ===============================

    runner = BacktestRunner(
        strategy=strategy,
        selector=selector,
        initial_cash=100000,
        # cash 使用 CASH_INSTRUMENT_ID (0)，无需配置
        slippage=0.005,
        transaction_cost=0.001,
        exchange_cost=0.0005,
        reinvest_ratio=0.98,
        universe_provider=lambda d: universe_all_tradable(conn, d),
        rebalance_day="last",  # 每月最后一个交易日调仓
        market="US",
    )

    runner.run(
        conn,
        start_date="2019-01-01",
        end_date="2026-02-28",
        overwrite=True,
    )

    conn.commit()
    conn.close()

    print("✅ Backtest completed.")
