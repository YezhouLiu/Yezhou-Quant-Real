# Feature Specification: 因子计算引擎（Factor Computation Engine）

**Feature Branch**: `003-factor-computation`  
**Created**: 2026-04-21  
**Status**: ✅ Implemented（6 个技术因子全部可运行）  
**Input**: 从 market_prices 表读取价格序列，输出到 factor_values 表

---

## User Scenarios & Testing

### User Story 1 - 单因子计算与入库（Single Factor Compute & Store）(Priority: P1)

对给定日期范围内的所有可交易证券，计算指定因子的跨截面值并批量写入数据库，支持增量更新和版本化追踪。

**Why this priority**: 因子是选股引擎的输入，是系统最核心的计算产出。

**Independent Test**: 对 2024 年全年运行 `compute_momentum()`，查询 `factor_values` 表中 `factor_name='mom_63d_skip21'`，应有约 252 × N 条记录（N=可交易股票数），且 factor_value 均为有限浮点数。

**Acceptance Scenarios**:

1. **Given** market_prices 有 2020-2024 的数据，**When** `compute_momentum(start_date='2024-01-01', end_date='2024-12-31')`，**Then** factor_values 中出现每个可交易 instrument 在每个交易日的 `mom_63d_skip21` 值
2. **Given** 某 ticker 数据不足 lookback 天数，**When** 计算，**Then** 该 ticker 该日的值为 NaN，不写入数据库（NaN 在 insert 前被过滤）
3. **Given** 同因子已有旧版本数据，**When** 参数不变重新计算，**Then** 通过 ON CONFLICT DO UPDATE 幂等更新
4. **Given** 不同参数（window=20 vs window=60）定义了两个版本，**When** 同时存在，**Then** 通过不同 factor_version 区分，互不干扰

---

### User Story 2 - 全因子批量计算（Compute All Factors）(Priority: P1)

一键运行所有 6 个已注册因子的计算，支持指定日期范围，适合每日增量更新和批量历史回补。

**Why this priority**: 每日 `daily_update()` 流程依赖此步骤，确保因子数据及时更新。

**Independent Test**: 运行 `compute_all_factors(date='2024-12-31')`，查询 factor_values 中 date='2024-12-31' 的所有 factor_name，应出现 6 种不同因子名。

**Acceptance Scenarios**:

1. **Given** 价格数据已覆盖到昨日，**When** `compute_all_factors(date=today())`，**Then** 所有 6 个因子在 factor_values 中有当日记录
2. **Given** 某因子计算报错（如数据问题），**When** 该因子失败，**Then** 其他因子仍继续计算（非原子性，各因子独立）
3. **Given** 历史回补场景，**When** `compute_all_factors(start_date='2020-01-01', end_date='2023-12-31')`，**Then** 所有日期的 6 个因子均写入

---

### User Story 3 - 信号归一化（Signal Normalization）(Priority: P1)

将原始因子值转换为跨截面可比的归一化信号，支持排名归一化（rank）和量级归一化（magnitude），适配信号合成和评分。

**Why this priority**: 原始因子值量纲不同（百分比 vs 对数）；归一化是多因子合成的前提。

**Independent Test**: 给定一列原始 momentum 值（正态分布），`rank_normalize()` 后所有值应落在 [-1, 1] 区间，median≈0。

**Acceptance Scenarios**:

1. **Given** 一批跨截面因子值（宽表），**When** `normalize_cross_section(df, specs)` 运行，**Then** 每列值域在 [-1, 1]，NaN 股票被自动过滤
2. **Given** factor_spec.ascending=False（如波动率，越低越好），**When** rank_normalize，**Then** 低值得分高，高值得分低（排序反转）
3. **Given** 使用 magnitude 归一化，**When** 存在极端离群值（>6σ），**Then** 被 clip 截断，不影响整体分布

---

### Edge Cases

- 所有股票在该日均无价格数据 → 返回空 DataFrame，跳过写入，记录警告日志
- 因子值全为相同值（零方差）→ rank_normalize 可处理（所有值映射到 0）
- 计算时窗口长度超过实际数据点数 → 该 ticker 的窗口期内返回 NaN，正常跳过

---

## Requirements

### Functional Requirements

- **FR-001**: 系统 MUST 支持以下 6 个技术因子：动量、波动率、美元成交量、最大回撤、波动率之波动率、跳跃风险
- **FR-002**: 每个因子 MUST 接受 `start_date`, `end_date` 参数，并从 DB 独立加载所需价格
- **FR-003**: 因子计算 MUST 自动加载 `buffer_days`（用于滚动窗口的预热期），窗口外的结果不写入
- **FR-004**: 因子写入 MUST 携带 `factor_version`（基于参数确定性生成）和 `factor_args`（JSONB 格式）
- **FR-005**: NaN 值 MUST 在 batch_insert 前过滤，不写入数据库
- **FR-006**: `normalize_cross_section()` MUST 支持 `ascending` 方向参数和 `rank`/`magnitude` 两种归一化方法
- **FR-007**: `magnitude_normalize()` MUST 使用 robust z-score（基于 MAD），并支持 tanh/sigmoid 激活函数

### Key Entities

- **FactorSpec**: 因子规格（factor_name, ascending, methods=['rank'/'mag']）
- **FactorFrame**: 从 DB 加载的长表，列：(instrument_id, date, factor_value)
- **SignalMatrix**: 归一化后的宽表，index=instrument_id, columns=factor_name

---

## Factor Reference

| 因子名 | 公式 | 参数 | 方向 |
|--------|------|------|------|
| `mom_{lookback}d_skip{skip}` | price[t-skip] / price[t-skip-lookback] - 1 | lookback, skip | ascending（高动量=好）|
| `vol_{window}d_ann{ann}` | rolling_std(log_returns) × √ann | window, annualize | descending（低波动=好）|
| `dv_{window}d_log` | log(rolling_mean(adj_close × adj_volume)) | window | ascending（高流动=好）|
| `mdd_{window}d` | rolling_min(price/rolling_max - 1) | window | descending（低回撤=好）|
| `vol_of_vol_*` | std(rolling_vol) | - | descending |
| `jump_risk_*` | gap risk metric | - | descending |

---

## Success Criteria

### Measurable Outcomes

- **SC-001**: 全量计算 1,000 只股票 × 252 天的动量因子（1 年）< 5 分钟
- **SC-002**: `get_factor_snapshot(date, factor_name)` 在 100 万条记录时查询 < 2 秒
- **SC-003**: 归一化后的信号矩阵，每列值域严格在 [-1, 1]
- **SC-004**: 增量计算（只计算新增日期）不影响历史因子值

---

## Assumptions

- 因子计算基于 `adj_close`（前复权价格），Tiingo 已处理企业行为
- `is_tradable=True` 的 instrument 才参与因子计算；退市股不计算
- 因子版本由参数组合确定性生成，相同参数的重复计算产生相同 version key
- 不同因子互相独立，可并行计算（但目前串行执行）
- 目前所有 6 个因子均为技术因子；基本面因子（E/P, ROE 等）为未来扩展
