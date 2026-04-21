# Feature Specification: 可交易宇宙管理（Trading Universe Management）

**Feature Branch**: `005-trading-universe`  
**Created**: 2026-04-21  
**Status**: ✅ Implemented（价格/成交量过滤 + 动态 is_tradable 同步）  
**Input**: instruments 表 + market_prices 表 + csv/tradable_candidates.csv

---

## User Scenarios & Testing

### User Story 1 - 动态可交易宇宙更新（Dynamic Universe Update）(Priority: P1)

每日收盘后，系统根据最新价格和成交量数据重新评估每只股票是否满足可交易条件（最低价格、最低成交量、是否在候选列表中），更新 `instruments.is_tradable` 标志。

**Why this priority**: 宇宙的质量直接决定因子计算和回测的样本质量；错误的宇宙导致低流动性股票污染信号。

**Independent Test**: 将某 ticker 的近期价格设为 $3（低于 $5 价格底线），运行 `update_tradable_universe()`，该 ticker 的 `is_tradable` 被设置为 False。

**Acceptance Scenarios**:

1. **Given** AAPL 在 tradable_candidates.csv 中且收盘价 > $5，**When** `update_tradable_universe()`，**Then** `is_tradable=True`
2. **Given** SOME_PENNY 价格 = $2（低于 price_floor=$5），**When** 更新，**Then** `is_tradable=False`
3. **Given** ticker 不在 tradable_candidates.csv 中，**When** 更新，**Then** `is_tradable=False`（即使价格满足条件）
4. **Given** price_floor 在 `config.yaml` 改为 $7，**When** 更新，**Then** 系统使用新的 $7 阈值，而非硬编码 $5

---

### User Story 2 - 初次宇宙初始化（Initial Universe Bootstrap）(Priority: P1)

系统首次部署时，从 tradable_candidates.csv 批量导入 instrument 主数据，建立初始宇宙。

**Why this priority**: 没有初始宇宙，价格下载和因子计算无从开始。

**Independent Test**: 从空 instruments 表开始，运行初始化脚本，instruments 表应有与 tradable_candidates.csv 等量的记录（约 2000-3000 只）。

**Acceptance Scenarios**:

1. **Given** 空 instruments 表，**When** 运行宇宙初始化，**Then** CSV 中所有 ticker 都以 `is_tradable=True` 写入 instruments 表
2. **Given** 某 ticker 已存在（部分初始化后重跑），**When** 再次运行，**Then** upsert 不产生重复行
3. **Given** CSV 包含 ETF 标记的行，**When** 导入，**Then** `asset_type='ETF'` 而非 `'Stock'`

---

### User Story 3 - 全量 US 股票列表维护（All US Stocks List）(Priority: P2)

定期从 Tiingo 下载官方支持的全量 US 股票列表，存为 `csv/all_us_stocks_listed.csv`，作为宇宙筛选的参考底库。

**Why this priority**: 全量列表是筛选可交易候选时的数据源之一，确保无遗漏。

**Independent Test**: 运行 `download_tiingo_supported_tickers()`，CSV 文件应有 > 50,000 行（包含 NASDAQ + NYSE + AMEX 所有上市股票）。

**Acceptance Scenarios**:

1. **Given** 网络正常，**When** `download_tiingo_supported_tickers()`，**Then** 生成含 ticker、exchange、assetType 等列的 CSV
2. **Given** 文件已存在，**When** 重新运行，**Then** 覆盖旧文件（最新快照）
3. **Given** 不需要 API Token，**When** 使用公开端点下载 supported_tickers.zip，**Then** 正常解压并解析

---

### Edge Cases

- CSV 中某 ticker 不在 Tiingo 支持列表中 → 仍写入 instruments，但下载价格时会得到 404
- 股票退市后（delist_date 有值）→ `is_tradable=False`，不再参与因子计算
- price_floor 和 volume_floor 均满足，但 ticker 非 US 股票 → 通过 exchange 过滤排除
- CSV 完全为空或下载失败 → 不清空现有的 is_tradable 状态，保留上次有效状态

---

## Requirements

### Functional Requirements

- **FR-001**: `update_tradable_universe()` MUST 从 `config.yaml` 读取 `price.floor`，而非硬编码
- **FR-002**: 系统 MUST 支持两级过滤标准：①在 tradable_candidates.csv 中 ②最近 N 日价格和成交量满足阈值
- **FR-003**: `instruments.is_tradable` MUST 每日批量更新（UPDATE WHERE instrument_id IN (...)）
- **FR-004**: 批量导入时 MUST 使用 ON CONFLICT DO UPDATE 保证幂等性
- **FR-005**: `tradable_candidates.py` MUST 合并多数据源（Russell/S&P/iShares），去重后输出
- **FR-006**: 手动添加的 ticker（标记为 manual source）MUST 在重新下载时保留（preserve_manual=True）

### Key Entities

- **TradableFilter**: 过滤规则组合（price_floor, volume_floor, lookback_days, must_be_in_candidates）
- **InstrumentUpsert**: 写入 instruments 的数据行（ticker, exchange, asset_type, status, is_tradable）

---

## Success Criteria

### Measurable Outcomes

- **SC-001**: `update_tradable_universe()` 在 5,000 只 instruments 规模下完成更新 < 30 秒
- **SC-002**: price_floor 修改后，下次 update 立即生效，无需重启
- **SC-003**: tradable_candidates.csv 合并后去重，最终宇宙大小在 1,500–3,000 只之间（正常市场规模）

---

## Assumptions

- `price.floor=$5.0` 是标准配置（已在 config.yaml 统一，不再硬编码）
- 宇宙更新基于最近 20 天（可配置）的平均价格和成交量，而非单日快照
- 退市（delist_date IS NOT NULL）的股票自动排除出宇宙，不需手动操作
- Russell 1000 是必须包含的最小宇宙；其他来源视为补充
