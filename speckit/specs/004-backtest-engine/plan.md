# Implementation Plan: 回测引擎

**Branch**: `004-backtest-engine` | **Date**: 2026-04-21 | **Status**: ✅ Implemented  
**Spec**: [spec.md](spec.md)

---

## Summary

实现一个月度再平衡回测引擎，采用 Strategy-Scorer-Selector-Portfolio 四层架构：ScoringStrategy 协调因子信号生成，LinearScorer 做多因子加权评分，TopKSelector 实施选股，Portfolio 管理持仓和成本扣除，BacktestRunner 驱动时间循环，每日快照写入 `exp_positions` 表。

---

## Technical Context

| 项目 | 值 |
|------|----|
| **Language/Version** | Python 3.10+ |
| **Primary Dependencies** | pandas, numpy, psycopg2 |
| **Storage** | 读：factor_values + market_prices；写：exp_positions |
| **Testing** | pytest（tests/engine/） |
| **Target Platform** | Windows/Linux 本地 |
| **Project Type** | 计算 library + task runner |
| **Performance Goals** | 5 年回测 < 10 分钟 |
| **Constraints** | 收盘价交易假设（T+0 收盘价再平衡） |

---

## Project Structure

```text
engine/
├── backtest_runner.py          # 核心回测循环（日期迭代 + 再平衡触发）
├── portfolio.py                # Portfolio dataclass（持仓状态 + 再平衡逻辑）
├── constants.py                # CASH_INSTRUMENT_ID=0 等常量
├── signals.py                  # 信号管道（DB → 长表 → 宽表 → 归一化）
├── normalizer.py               # robust_zscore() 数学工具
├── strategies/
│   └── scoring_strategy.py    # ScoringStrategy（组合 factor_specs + scorer）
├── scorers/
│   ├── base.py                # Scorer Protocol（接口定义）
│   └── linear.py              # LinearScorer（加权线性评分）
└── selectors/
    ├── base.py                # Selector Protocol（接口定义）
    └── topk.py                # TopKSelector（Top-K + 过滤器）

tasks/
└── backtest_tasks.py           # 策略定义入口（用户配置点）
```

---

## Architecture Overview

```
BacktestRunner.run(conn, start_date, end_date, overwrite)
  │
  ├─ 遍历 trading_calendar（start→end 所有交易日）
  │    │
  │    └─ 每日执行：
  │         ├─ [Is Rebalance Day?] → 触发 rebalance()
  │         │    ├─ ScoringStrategy.score_for_date(date)
  │         │    │    ├─ build_signals_for_date(factor_specs)
  │         │    │    │    ├─ DB: get_factor_values()
  │         │    │    │    └─ normalize_cross_section()
  │         │    │    └─ LinearScorer.score(signals) → scores DataFrame
  │         │    │
  │         │    ├─ TopKSelector.select(scores) → [instrument_ids]
  │         │    │
  │         │    └─ Portfolio.rebalance(target_ids, prices)
  │         │         ├─ 计算目标权重（等权）
  │         │         ├─ 卖出不在目标中的持仓（扣成本）
  │         │         ├─ 买入新目标（扣成本）
  │         │         └─ 保留 (1 - reinvest_ratio) 现金
  │         │
  │         └─ Portfolio.snapshot() → exp_positions 写入
  │
  └─ Done
```

---

## Portfolio.rebalance() Algorithm

```python
def rebalance(self, target_ids, prices, config):
    # 1. 计算当前总 NAV
    nav = self.total_value(prices)
    
    # 2. 计算可投资金额（保留现金缓冲）
    investable = nav * config.reinvest_ratio
    
    # 3. 等权目标：each = investable / len(target_ids)
    target_value_per_stock = investable / len(target_ids)
    
    # 4. 卖出不在目标中的持仓
    for inst_id in (self.positions.keys() - set(target_ids)):
        sell_proceeds = position.market_value
        sell_cost = sell_proceeds * (slippage + tx_cost + ex_cost)
        self.cash += sell_proceeds - sell_cost
    
    # 5. 买入目标中的新持仓（或调整现有持仓）
    for inst_id in target_ids:
        buy_amount = target_value_per_stock
        buy_cost = buy_amount * (slippage + tx_cost + ex_cost)
        quantity = (buy_amount - buy_cost) / prices[inst_id]
        self.positions[inst_id] = Position(quantity, ...)
        self.cash -= buy_amount
```

---

## Rebalance Day Resolution

| `rebalance_day` 值 | 触发规则 |
|-------------------|---------|
| `'last'` | 当月最后一个交易日 |
| `'first'` | 当月第一个交易日 |
| `int` (1-28) | 当月第 N 日；若非交易日则顺延至下一交易日 |

---

## Transaction Cost Summary

| 成本项 | 默认值 | 应用时机 |
|--------|--------|---------|
| slippage | 0.50% | 每笔买入/卖出交易 |
| transaction_cost | 0.10% | 每笔买入/卖出交易（佣金） |
| exchange_cost | 0.05% | 每笔买入/卖出交易（交易所费用） |
| **合计** | **0.65%** | 单边成本（round-trip = 1.30%） |

---

## LinearScorer Implementation

```python
@dataclass
class LinearScorer:
    weights: dict[str, float]  # {column_name: weight}
    bias: float = 0.0
    post_transform: str = None  # None | 'tanh' | 'sigmoid' | 'rank'
    
    def score(self, signals: DataFrame) -> DataFrame:
        score = sum(w * signals[col] for col, w in self.weights.items()) + bias
        if post_transform == 'tanh':
            score = np.tanh(score)
        return score
```

---

## TopKSelector Implementation

```python
@dataclass
class TopKSelector:
    k: int
    sort_by: str = 'score'
    sort_ascending: bool = False
    filters: list[tuple] = None  # [(col, op, val), ...]
    
    def select(self, signals: DataFrame):
        df = signals.copy()
        for (col, op, val) in (filters or []):
            df = df[OPERATOR_MAP[op](df[col], val)]
        return df.nlargest(k, sort_by).index.tolist()
```

---

## Constitution Check

| 原则 | 状态 | 说明 |
|------|------|------|
| I. 数据优先 | ✅ | 从 DB 读价格/因子，结果写回 DB |
| V. 模块化信号管道 | ✅ | 严格分层：信号→评分→选股→组合 |
| VII. 成本真实性 | ✅ | 滑点/佣金/交易所费均已实现 |
| VI. 回测/实盘分离 | ✅ | exp_positions vs positions 分开 |

---

## Future Extensions（方向 B：回测分析体系）

| 扩展 | 状态 |
|------|------|
| Sharpe / Sortino / Calmar Ratio | 🔴 未实现 |
| Alpha / Beta vs SPY | 🔴 未实现 |
| IC / ICIR 因子有效性验证 | 🔴 未实现 |
| 分位数收益分析 | 🔴 未实现 |
| 月度换手率 / 持仓集中度 | 🔴 未实现 |
| 按估值倒数加权（非等权） | 🔴 未实现（方向 C） |
