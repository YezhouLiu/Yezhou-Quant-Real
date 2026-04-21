# Feature Specification: 回测引擎（Backtest Engine）

**Feature Branch**: `004-backtest-engine`  
**Created**: 2026-04-21  
**Status**: ✅ Implemented（月度再平衡，等权重，含成本模型）  
**Input**: factor_values + market_prices → exp_positions（NAV 时间序列）

---

## User Scenarios & Testing

### User Story 1 - 策略定义与因子信号配置（Strategy Configuration）(Priority: P1)

用户通过 Python API 定义多因子策略：指定因子及其方向/权重，配置选股器（Top-K），指定回测时间范围和再平衡频率，一键提交回测。

**Why this priority**: 策略配置是回测的入口，设计良好的 API 直接决定系统的可用性和可扩展性。

**Independent Test**: 在 `backtest_tasks.py` 中定义一个三因子策略，调用 `run_backtest(conn, strategy, start='2020-01-01', end='2024-12-31')`，5 年回测无报错完成。

**Acceptance Scenarios**:

1. **Given** 用户定义 `factor_specs = [FactorSpec('mom_63d', ascending=True, methods=['rank'])]`，**When** 回测运行，**Then** 每个再平衡日按该因子排名选出 Top-K 股票
2. **Given** 多因子（动量 50% + 波动率 30% + MDD 20%），**When** 通过 `LinearScorer(weights={...})`，**Then** 合成分 = Σ(weight × signal)
3. **Given** `TopKSelector(k=5, sort_by='score')`，**When** 打分后，**Then** 每次再平衡选出分数最高的 5 只股票
4. **Given** 添加过滤器 `filters=[('dv_20d_log', '>', 10)]`，**When** 选股，**Then** 不满足美元成交量条件的股票被排除

---

### User Story 2 - 月度再平衡执行（Monthly Rebalance Execution）(Priority: P1)

回测引擎按配置的频率（默认月末）触发再平衡，以当日因子信号为依据调整持仓，考虑真实交易成本。

**Why this priority**: 月度再平衡是慢量化投资的核心执行逻辑，必须准确计算换手和成本。

**Independent Test**: 2020-01-01 至 2020-03-31 的回测，应在 2020-01-31 和 2020-02-28 各触发一次再平衡，`exp_positions` 中这两个日期后的持仓应发生变化。

**Acceptance Scenarios**:

1. **Given** `rebalance_day='last'`（月末）, **When** 2024-04-30 是交易日，**Then** 该日触发再平衡
2. **Given** `rebalance_day='last'` 且月末为非交易日（如2024-11-30 周六），**When** 回测运行，**Then** 向前回退至最近交易日（11-29）触发
3. **Given** `rebalance_day=15`（指定固定日期），**When** 每月15日触发，**Then** 若15日为非交易日则顺延至下一交易日
4. **Given** 再平衡时某持仓股票无当日价格，**When** 调整权重，**Then** 该股票被跳过，其余正常再平衡

---

### User Story 3 - 组合价值计算与 NAV 输出（Portfolio NAV）(Priority: P1)

每个交易日结束时，用当日收盘价更新所有持仓的市值，计算总 NAV，将快照写入 `exp_positions` 表。

**Why this priority**: NAV 时间序列是衡量策略收益的基础，也是与基准比较和可视化的输入。

**Independent Test**: 对 100 天回测区间调用 `get_exp_nav(conn)`，返回的 NavPoint 列表应有 ≤100 个 date，每个 date 的 NAV = cash + Σ(quantity × price)。

**Acceptance Scenarios**:

1. **Given** 初始资金 $100,000，持有 5 只均权股票，**When** 一月后所有股票上涨 10%，**Then** NAV ≈ $110,000（扣除交易成本）
2. **Given** NAV 快照存于 `exp_positions`，**When** 调用 `get_exp_nav(conn)`，**Then** 返回按日期升序的 (date, nav) 序列，无缺日
3. **Given** 再平衡后持仓调整，**When** 计算当日 NAV，**Then** 现金 + 各股市值之和与再平衡前基本相等（仅差交易成本）

---

### User Story 4 - 交易成本模型（Transaction Cost Model）(Priority: P2)

再平衡时准确计算滑点、佣金、交易所费用，从组合 NAV 中扣除，避免回测过于乐观。

**Why this priority**: 无成本回测高估真实收益；成本模型是策略可行性验证的必要条件。

**Independent Test**: 对单次再平衡，以 $10,000 买入一只股票，预期扣除成本 = $10,000 × (0.5% + 0.1% + 0.05%) = $65，NAV 对应减少。

**Acceptance Scenarios**:

1. **Given** `slippage=0.005, transaction_cost=0.001, exchange_cost=0.0005`，**When** 买入 $10,000，**Then** 实际成本 = $10,000 × 0.0065 = $65 被从现金扣除
2. **Given** `reinvest_ratio=0.98`（保留 2% 现金缓冲），**When** 再平衡，**Then** 最多使用 98% 的 NAV 持仓，2% 保持现金
3. **Given** 卖出旧持仓后现金增加，**When** 买入新持仓，**Then** 买卖两侧各自计算成本，不做净额扣减

---

### Edge Cases

- 初始日期无因子数据 → 保持全现金，不报错，记录警告
- Top-K 筛选后候选股票不足 K 只 → 仅持有满足条件的股票，权重按实际股票数均分
- 某股票再平衡日停盘（无价格）→ 保留原持仓，不重新平衡该股票
- NAV 跌至接近 0 → 系统不强制清仓，继续按持仓计算

---

## Requirements

### Functional Requirements

- **FR-001**: 系统 MUST 提供 `FactorSpec` 数据类，包含 factor_name、ascending、methods 字段
- **FR-002**: 系统 MUST 提供 `LinearScorer`，支持对多因子信号列做加权线性组合
- **FR-003**: 系统 MUST 提供 `TopKSelector`，支持 k、sort_by、filters 参数
- **FR-004**: `BacktestRunner.run()` MUST 接受 `rebalance_day`（'first'/'last'/int），并自动解析至最近交易日
- **FR-005**: 再平衡时 MUST 扣除 slippage + transaction_cost + exchange_cost
- **FR-006**: 再平衡时 MUST 保留 `(1-reinvest_ratio)` × NAV 为现金缓冲
- **FR-007**: 每日 NAV 快照 MUST 写入 `exp_positions`，包含现金行（instrument_id=0）
- **FR-008**: 回测 MUST 支持 `overwrite=True`，可清空并重新写入 `exp_positions` 数据

### Key Entities

- **Portfolio**: 组合状态（cash, positions: Dict[int→Position], 方法: total_value, rebalance, snapshot）
- **Position**: 单只证券头寸（instrument_id, quantity, cost_basis, current_price, market_value）
- **ScoringStrategy**: 策略组合器（factor_specs + scorer → 每日评分）
- **FactorSpec**: 因子规格（factor_name, ascending, methods）
- **LinearScorer**: 线性评分器（weights: {column_name → float}）
- **TopKSelector**: 选股器（k, sort_by, sort_ascending, filters）

---

## Success Criteria

### Measurable Outcomes

- **SC-001**: 5 年回测（2019-2024），1000 只股票宇宙，Top-10 持仓，月度再平衡，运行时间 < 10 分钟
- **SC-002**: 回测结果 NAV 曲线图与 SPY 基准可在 matplotlib 中正确绘制
- **SC-003**: 再平衡日的持仓变化表现在 exp_positions 中，前后日期的 market_value 之和不超过 0.01% 偏差（仅差成本）
- **SC-004**: `overwrite=True` 时，原有 exp_positions 数据被完整清除后重新写入

---

## Assumptions

- 现阶段只支持等权重（equal weight）；按估值倒数加权等方案为未来扩展
- 回测假设可以在再平衡日以当日收盘价成交（无执行延迟）
- 成本单向计算（买入和卖出各自付出全额成本，非净额）
- 不处理杠杆、做空；组合始终为多头
- 目前无 Sharpe/Sortino/IC 等绩效指标计算；为方向 B 扩展内容
