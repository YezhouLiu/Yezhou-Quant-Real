# Feature Specification: 任务调度与可视化（Task Orchestration & Visualization）

**Feature Branch**: `006-task-orchestration`  
**Created**: 2026-04-21  
**Status**: ✅ Implemented（手动触发）| ⚠️ 自动调度未实现  
**Input**: 所有下游模块（下载/因子/回测），输出：exp_positions NAV + matplotlib 图表

---

## User Scenarios & Testing

### User Story 1 - 每日更新流水线（Daily Update Pipeline）(Priority: P1)

用户在收盘后运行每日更新任务，系统自动按顺序执行：①增量价格下载 ②计算今日因子 ③更新可交易宇宙，三步完成即可保持数据最新。

**Why this priority**: 每日更新是整个系统的"接地线"，确保决策所用数据不过时。

**Independent Test**: 运行 `daily_update(conn)`，查看 `data_update_logs` 中出现当日的三条更新记录（prices, factors, universe），且每条 status='success'。

**Acceptance Scenarios**:

1. **Given** 当前数据截止昨日，**When** 运行 `daily_update()`，**Then** prices/factors/universe 均更新至当日
2. **Given** 价格下载步骤失败（网络问题），**When** `daily_update()` 捕获异常，**Then** 记录失败日志，因子计算步骤被跳过（勿用过期价格算因子）
3. **Given** 非交易日（周末/节假日），**When** 运行 `daily_update()`，**Then** 系统识别后跳过（或最多做一次空操作），不报错

---

### User Story 2 - 回测任务执行（Backtest Task Execution）(Priority: P1)

用户通过 `backtest_tasks.py` 定义策略参数（因子组合、选股器、时间范围），一键执行完整回测，结果写入 `exp_positions`，立即可视化 NAV 曲线。

**Why this priority**: 这是系统"研究产出"的核心路径。

**Independent Test**: 修改策略参数重新运行 `run_backtest()`，`exp_positions` 中的数据应被清空后重写（overwrite=True），最终 NAV 与预期方向（多头 vs SPY）一致。

**Acceptance Scenarios**:

1. **Given** 已定义三因子策略（动量/波动率/MDD），**When** `run_backtest(overwrite=True)`，**Then** exp_positions 表有完整的 2019-2026 日度快照
2. **Given** 回测完成，**When** 调用 `compare_portfolio_with_tickers(['SPY', 'QQQ'])`，**Then** NAV 图像正确显示回测组合与两个基准的对比
3. **Given** 不同 k 值（Top-5 vs Top-10），**When** 分别回测，**Then** 两次结果以不同颜色显示在同一图上

---

### User Story 3 - 投资组合 NAV 可视化（Portfolio NAV Visualization）(Priority: P2)

从 `exp_positions` 加载回测结果，归一化为基准=100 的收益率曲线，与 SPY/QQQ 等 ticker 对比绘图。

**Why this priority**: 数字要变成图，才能直观评估策略表现。

**Independent Test**: `compare_portfolio_with_tickers(['SPY'])` 生成一个 matplotlib 图，x 轴为日期，y 轴为归一化 NAV（起始=100），两条线分别代表回测组合和 SPY。

**Acceptance Scenarios**:

1. **Given** exp_positions 有 5 年数据，**When** 运行可视化，**Then** x 轴覆盖完整日期范围，无断点
2. **Given** SPY 的价格通过 `TickerNAVSource` 从 market_prices 读取，**When** 对齐日期后绘图，**Then** 两者起始值均归一化为 100
3. **Given** 回测期间某段无数据（如节假日），**When** 绘图，**Then** 用前向填充（forward fill）保持曲线连续

---

### User Story 4 - 季度/年度批处理任务（Seasonal & Annual Tasks）(Priority: P3)

每季度末触发基本面数据下载，每年末进行年度汇总（如更新宇宙、清理过期数据）。

**Why this priority**: 低频任务不需要精确实时，但需要有明确的触发点和执行记录。

**Independent Test**: 手动运行 `seasonal_update()`，`data_update_logs` 有当季的 SEC EDGAR 下载记录，`fundamental_data` 表有新增季报数据。

**Acceptance Scenarios**:

1. **Given** 季度末（3/6/9/12 月底），**When** `seasonal_update()` 被手动或计划任务触发，**Then** SEC EDGAR 下载执行并记录日志
2. **Given** `annual_tasks()` 运行，**When** 执行，**Then** 宇宙 CSV 重新从 Wikipedia 拉取，instruments 表更新

---

### Edge Cases

- `exp_positions` 表已有数据且 `overwrite=False` → 报错提示，不继续运行，要求显式指定 overwrite
- 回测时间范围内市场价格数据缺口 →记录日志，用最近可用价格（forward fill）估算 NAV
- matplotlib 在无头服务器环境缺少 Display → 保存为 PNG 文件而非 show()

---

## Requirements

### Functional Requirements

- **FR-001**: `daily_update()` MUST 按固定顺序执行：download_prices → compute_all_factors → update_tradable_universe
- **FR-002**: `run_backtest()` MUST 支持 `overwrite` 参数，True 时先清空 exp_positions
- **FR-003**: `compare_portfolio_with_tickers()` MUST 支持多 ticker 基准（如 ['SPY', 'QQQ', 'IWM']）
- **FR-004**: NAV 归一化 MUST 以回测起始日为基准值 100
- **FR-005**: `PortfolioNAVSource.load()` MUST 从 exp_positions 聚合每日 SUM(market_value) 作为 NAV
- **FR-006**: `TickerNAVSource.load()` MUST 从 market_prices 读取指定 ticker 的 adj_close，并归一化

### Key Entities

- **DailyPipeline**: 三步串行任务（prices → factors → universe）
- **BacktestTask**: 策略配置容器（factor_specs, scorer, selector, runner params）
- **NAVSeries**: 日期→NAV 的 Series（用于绘图对比）

---

## Success Criteria

### Measurable Outcomes

- **SC-001**: `daily_update()` 在 1000 只股票宇宙下完成全流程 < 30 分钟
- **SC-002**: NAV 可视化图表加载时间 < 5 秒
- **SC-003**: 回测完成后无需额外操作即可运行可视化（一键端到端）

---

## Assumptions

- 目前所有任务均为手动触发；自动调度（APScheduler / Windows Task Scheduler）为方向 F 的扩展内容
- matplotlib 在交互式环境（Jupyter/VS Code）中通过 `show()` 展示，非交互式保存为文件
- NAV 可视化只需要日线精度；不需要分钟级或实时更新
