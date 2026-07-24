# Feature Specification: 每日市场情报简报（Daily Market Intelligence Briefing）

**Feature Branch**: `007-daily-briefing`  
**Created**: 2026-07-24  
**Status**: ✅ Implemented  
**Input**: `factor_values` 表 + `market_prices` 表 + `instruments.sector`  
**Output**: 分类榜单文字报告，写入日志，可供人工研究

---

## 背景与动机

系统已积累 9 个技术因子，但原有流程只能输出"僵化的选股 Top-K 名单"。
实际研究需求更接近于：**每天给我一份市场简报，告诉我谁在发生有趣的事**，
由人工判断是否深入研究（抄底、避雷、事件驱动等），而非强制执行自动化交易信号。

---

## User Scenarios & Testing

### User Story 1 - 动量榜单（谁最近涨/跌得最凶）(Priority: P1)

用户每日查看简报时，能快速识别近期涨幅最大和跌幅最大的标的，据此决定是否跟进或避雷。

**Why this priority**: 这是日内最基础的市场感知需求，直接来自价格数据，零延迟。

**Independent Test**: 在测试环境构造 30 只股票的 `mom_5d` 快照，生成简报后 `momentum_leaders` section 应有 ≤ top_n 行，第一行的 `mom_5d` 应为所有标的中最大值。

**Acceptance Scenarios**:

1. **Given** `factor_values` 中有昨日的 `mom_5d` 数据，**When** `run_briefing()` 运行，**Then** 日志中出现按 `mom_5d` 降序排列的前 `top_n` 只股票，包含 1d/5d/1m 涨跌幅
2. **Given** 同样的数据，**When** 查看 LAGGARDS 榜，**Then** 出现按 `mom_5d` 升序排列的标的（跌幅最大的）
3. **Given** 某标的缺失 `mom_5d` 数据，**When** 生成简报，**Then** 该标的被自动跳过，不影响其他标的的排名

---

### User Story 2 - 量突变探测（谁今天有事件驱动）(Priority: P1)

用户通过量比异动发现"有事情发生"的标的，手动查询新闻后决定是否行动。

**Why this priority**: 量突变是事件驱动信号最早的可量化特征，优先于价格反应。

**Independent Test**: 构造 30 只股票，其中一只 `vol_ratio_20d = 8.0`，其余 < 2.0，运行简报后 `volume_spikes` section 只包含那一只股票。

**Acceptance Scenarios**:

1. **Given** 某标的今日量 = 20 日均量的 4 倍（`vol_ratio_20d = 4.0`），**When** 生成简报，**Then** 该标的出现在 VOLUME SPIKES section，显示倍数 `4.0x`
2. **Given** 所有标的 `vol_ratio_20d < 2.0`（默认阈值），**When** 生成简报，**Then** VOLUME SPIKES section 行数为 0（显示"no entries"）
3. **Given** 量比排名，**When** 生成简报，**Then** 按量比降序排列，同时显示当日 1d/5d 涨跌幅（辅助判断是放量上涨还是放量下跌）

---

### User Story 3 - 连跌预警（谁在持续暴跌）(Priority: P1)

用户识别连续下跌天数较多的标的，结合 5d 收益率，判断是否存在趋势性风险或抄底机会。

**Why this priority**: 连续下跌是结构性风险的最直观信号，人工判断成本极低。

**Independent Test**: 构造一只股票连续下跌 6 天（prices = [100, 99, 98, 97, 96, 95, 94]），运行 `calc_single_instrument_decline_streak`，最后一天的因子值应为 6。

**Acceptance Scenarios**:

1. **Given** 某标的 `decline_streak = 7`（连续 7 个交易日收盘价低于前日），**When** 生成简报，**Then** 该标的出现在 DECLINE STREAKS section，并同时显示 5d 收益率和最大回撤
2. **Given** 连续下跌不足 3 天（默认阈值），**When** 生成简报，**Then** DECLINE STREAKS section 为空
3. **Given** 上涨一天后又下跌，**When** 计算 `decline_streak`，**Then** 新的跌势从 1 重新计数（streak 正确重置）
4. **Given** 持平日（close == prev_close），**When** 计算，**Then** 不计入连跌，streak 重置为 0

---

### User Story 4 - 板块汇总（哪些板块在领涨/领跌）(Priority: P2)

用户每天快速了解各板块的整体动向，识别板块轮动信号。

**Why this priority**: 板块视角比个股视角更稳定，是宏观判断的基础。

**Independent Test**: 构造 5 个板块各 5 只股票，各板块内 `mom_5d` 均值不同，生成简报后 SECTOR SUMMARY 应按 avg_mom_5d 降序排列，且每行显示该板块 Top 3 领涨股票。

**Acceptance Scenarios**:

1. **Given** `instruments.sector` 已由 yfinance 填充，**When** 生成简报，**Then** SECTOR SUMMARY 按 avg_5d_return 降序排列各板块，每行显示标的数量、平均涨跌幅、Top 3 ticker
2. **Given** 某标的 `sector` 为空，**When** 汇总，**Then** 该标的被排除出板块汇总（不影响其他板块计算）

---

### User Story 5 - 波动率预警（谁最近波动异常剧烈）(Priority: P2)

识别年化波动率超过阈值（默认 50%）的标的，提示潜在不稳定性。

**Acceptance Scenarios**:

1. **Given** 某标的 `vol_20d_ann252 = 0.85`（85% 年化波动率），**When** 生成简报，**Then** 该标的出现在 VOLATILITY ALERTS section
2. **Given** 所有标的波动率 < 50%，**When** 生成简报，**Then** VOLATILITY ALERTS 为空

---

### Edge Cases

- `factor_values` 中没有任何数据 → 简报所有 section 为空，打印 warning，不崩溃
- 某 section 的数据不足 top_n → 返回实际有效行数，不报错
- `instruments.sector` 全部为空 → SECTOR SUMMARY section 不出现或为空，其余 section 正常生成
- 传入不存在的 `date` → `_load_factor_snapshot` 返回空 DataFrame，简报所有 section 为空

---

## Requirements

### Functional Requirements

- **FR-001**: 系统 MUST 每日生成包含 6 个分类榜单的简报：MOMENTUM LEADERS、MOMENTUM LAGGARDS、VOLUME SPIKES、DECLINE STREAKS、VOLATILITY ALERTS、SECTOR SUMMARY
- **FR-002**: 简报 MUST 仅使用 `factor_values` 和 `market_prices` 中已存在的数据，不触发任何新的计算或下载
- **FR-003**: `generate_briefing()` MUST 接受可选的 `conn`、`date`、`top_n` 参数；`date=None` 时自动使用 `factor_values` 中最新日期
- **FR-004**: `generate_briefing()` MUST 返回结构化 dict，`format_briefing()` 将其转换为人类可读字符串，两者 MUST 独立可测试
- **FR-005**: VOLUME SPIKES section MUST 只显示 `vol_ratio_20d >= 2.0` 的标的（可调阈值 `_VOL_SPIKE_MIN_RATIO`）
- **FR-006**: DECLINE STREAKS section MUST 只显示 `decline_streak >= 3` 的标的（可调阈值 `_DECLINE_STREAK_MIN_DAYS`）
- **FR-007**: `run_briefing()` MUST 将格式化简报写入系统日志（logger），并返回字符串供上层调用
- **FR-008**: `daily_tasks.py` 中的 `daily_update()` MUST 在 `compute_all_factors()` 完成后调用 `run_briefing()`

### New Factors Required

| 因子名 | 文件 | 描述 | 用于 section |
|--------|------|------|--------------|
| `mom_1d` | `factors/momentum.py`（扩展 SPECS） | 单日涨跌幅 | MOMENTUM 榜 |
| `vol_ratio_20d` | `factors/volume_ratio.py` | 今日量 / 过去20日均量 | VOLUME SPIKES |
| `decline_streak` | `factors/decline_streak.py` | 连续下跌交易日数 | DECLINE STREAKS |

---

## Briefing Output Format

```
========================================================================
  DAILY MARKET BRIEFING  —  2026-07-24
========================================================================
────────────────────────────────────────────────────────────────────────
  MOMENTUM LEADERS (Top by 5d Return)
────────────────────────────────────────────────────────────────────────
    #  Ticker    Company                   Sector              1d      5d      1m
    1.  NVDA      NVIDIA Corp               Technology      +3.2%  +12.1%   +8.3%
    2.  META      Meta Platforms            Technology      +1.5%   +9.4%   +6.1%
  ...

────────────────────────────────────────────────────────────────────────
  VOLUME SPIKES (Vol Ratio >= 2x, Top by Ratio)
────────────────────────────────────────────────────────────────────────
    #  Ticker    Company                   Sector        VolRatio      1d      5d
    1.  INTC      Intel Corp                Technology      15.3x   -8.2%  -14.1%

────────────────────────────────────────────────────────────────────────
  DECLINE STREAKS (>= 3 Consecutive Down Days)
────────────────────────────────────────────────────────────────────────
    #  Ticker    Company                   Sector          Streak   5d Ret     MDD
    1.  XYZ       XYZ Inc                   Energy             8d  -18.2%  -32.1%

────────────────────────────────────────────────────────────────────────
  SECTOR SUMMARY (Sorted by Avg 5d Return)
────────────────────────────────────────────────────────────────────────
  Sector                   N   Avg5d   Avg1m  Top Tickers
  Technology               52  +3.2%  +8.1%  NVDA, META, MSFT
  Healthcare               34  +1.1%  +2.4%  LLY, UNH, ABBV
```

---

## Adjustable Thresholds

| 常量 | 默认值 | 含义 |
|------|--------|------|
| `_VOL_SPIKE_MIN_RATIO` | `2.0` | 量比阈值，低于此值不出现在量突变榜 |
| `_DECLINE_STREAK_MIN_DAYS` | `3` | 连跌天数阈值，低于此值不出现在连跌榜 |
| `_VOL_ALERT_MIN` | `0.50` | 年化波动率阈值（50%），低于此值不出现在波动预警榜 |

---

## Success Criteria

- **SC-001**: `run_briefing()` 在 1000 只股票的宇宙中，单次运行 < 5 秒
- **SC-002**: 所有 section 的行数均 ≤ `top_n`（默认 20）
- **SC-003**: 生成的简报字符串包含所有 6 个 section 的关键词
- **SC-004**: 在没有任何因子数据的空库上运行，不抛出异常，返回空 sections

---

## Assumptions

- 简报不持久化（不写入数据库），仅通过日志输出
- 数据来源为已计算完成的 `factor_values`（简报在 `compute_all_factors()` 之后运行）
- 板块（`instruments.sector`）由 yfinance 填充，可能部分缺失
- 阈值为模块级常量，可直接编辑调整，无需 `config.yaml`（分析工具，非交易参数）
