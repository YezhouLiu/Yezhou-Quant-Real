# Feature Specification: 价格数据下载管道（Price Data Download Pipeline）

**Feature Branch**: `002-price-data-pipeline`  
**Created**: 2026-04-21  
**Status**: ✅ Implemented (Tiingo EOD) | ⚠️ Partial (SEC EDGAR – runnable, not integrated)  
**Input**: 慢量化系统需要可靠的日线价格数据和基本面数据

---

## User Scenarios & Testing

### User Story 1 - 增量下载日线价格（Daily Price Incremental Download）(Priority: P1)

系统每日收盘后自动从 Tiingo API 拉取当日及缺失的历史价格，写入 `market_prices` 表，从上次下载断点续传，不重复拉取已有数据。

**Why this priority**: 价格数据是因子计算的直接输入，缺价格则系统无法运作。

**Independent Test**: 在 `system_state` 表中设置 last_price_date=2024-01-01，运行 `download_prices()`，验证 `market_prices` 中出现 2024-01-02 之后的新数据，且 `system_state` 更新至最新日期。

**Acceptance Scenarios**:

1. **Given** `market_prices.max_date = 2024-06-30`，**When** `download_prices()` 在 2024-07-05 运行，**Then** 拉取 7/1–7/5 的数据并写入，`system_state.last_price_date` 更新至 2024-07-05
2. **Given** Tiingo API 返回 429（限速），**When** 下载器遇到限速，**Then** 自动指数退避重试，不终止下载
3. **Given** 500 只股票批量下载，**When** 其中 3 只 API 返回错误，**Then** 允许最多 1% 失败率下继续，错误写入 `data_update_logs`
4. **Given** 某 ticker 已退市，**When** Tiingo 返回空数据，**Then** 跳过该 ticker 不报错，记录日志

---

### User Story 2 - 可交易宇宙列表下载（Tradable Universe Download）(Priority: P1)

从 Russell 1000/2000 和 S&P 指数成分页面拉取可交易候选票池，生成 `csv/tradable_candidates.csv`，供宇宙管理模块使用。

**Why this priority**: 需要知道"应该下载哪些股票"才能进行有针对性的价格下载和因子计算。

**Independent Test**: 运行 `save_tradable_candidates_csv()`，产出 CSV 文件包含 1000+ 行，包含 ticker、exchange 列，且 Russell 1000 成分股全部包含。

**Acceptance Scenarios**:

1. **Given** 网络正常，**When** `save_tradable_candidates_csv(include_all_sources=True)`，**Then** CSV 包含 Russell 1000（必选）+ Russell 2000 + S&P 系列的合集，去重后无重复 ticker
2. **Given** CSV 中有手动添加的 ticker（manual entries），**When** 重新下载，**Then** `preserve_manual=True` 保留手动行，不覆盖
3. **Given** Wikipedia 某源页面结构改变，**When** 解析失败，**Then** 记录警告日志并跳过该源，其他源仍继续

---

### User Story 3 - SEC EDGAR 基本面下载（Fundamental Data Download）(Priority: P2)

从 SEC EDGAR XBRL API 拉取已上市公司的季度财务报表指标（EPS、净利润、营收等），写入 `fundamental_data` 表，每季度触发一次。

**Why this priority**: 基本面数据是价值投资因子的原始来源，完成后可解锁 E/P, ROE, FCF/P 等因子。

**Independent Test**: 对单只股票（如 AAPL）运行 `download_fundamentals()`，验证 `fundamental_data` 表中有 `metric_name='NetIncomeLoss'` 的季度记录，且 report_date 为实际财报日期。

**Acceptance Scenarios**:

1. **Given** AAPL 的 instrument_id 存在，**When** `download_fundamentals(tradable_only=True)`，**Then** fundamental_data 表中有 AAPL 的最新季报数据
2. **Given** SEC EDGAR 限速（每秒 10 次），**When** 遍历 1000 只股票，**Then** 每次请求间隔 ≥ 0.25 秒（`sleep_seconds=0.25`），不触发 429
3. **Given** 某公司无 XBRL 数据（如 ADR），**When** API 返回空，**Then** 跳过并记录在 `data_update_logs`
4. **Given** 上季度已下载，**When** 本季度再次运行，**Then** 仅拉取新报告期的数据（增量）

---

### User Story 4 - 行业/板块元数据填充（Sector/Industry Enrichment）(Priority: P3)

通过 yfinance 为 instruments 表中 `sector`/`industry` 为空的证券批量填充 GICS 行业分类数据。

**Why this priority**: 行业数据是行业中性策略和板块分析的前提，但不影响核心价格因子流程。

**Independent Test**: 选取 10 只 `sector=NULL` 的 instruments 运行 `fill_sector_industry_yfinance.py`，查询后 sector 均被填充，无 NULL 残留。

**Acceptance Scenarios**:

1. **Given** instruments 表中存在 sector=NULL 的记录，**When** 运行脚本，**Then** 通过 yfinance 查询并 UPDATE 对应记录
2. **Given** yfinance 查询某 ticker 失败，**When** 遇到异常，**Then** 跳过该 ticker，继续处理其他
3. **Given** 所有 instruments 的 sector 已填充，**When** 再次运行，**Then** 不发出无效请求，幂等执行

---

### Edge Cases

- API Key 无效 → 在启动时检查，立即抛出 ConfigError，不进入下载循环
- 网络完全断开 → 捕获连接异常，写入 `data_update_logs` 并 exit code=1
- 数据库写入时外键约束失败（instrument_id 不存在）→ 跳过该批次并记录错误

---

## Requirements

### Functional Requirements

- **FR-001**: `download_prices()` MUST 从 `system_state` 读取 `last_price_date` 作为起始日期
- **FR-002**: 系统 MUST 支持批量大小配置（`batch_size=500`），避免单次请求过大
- **FR-003**: 系统 MUST 实现指数退避重试（HTTP 429/5xx），最大重试次数可配置
- **FR-004**: 失败率阈值 MUST 可配置（`max_failure_rate=0.01`），超过阈值终止下载
- **FR-005**: SEC EDGAR 下载器 MUST 遵守 0.25 秒/请求的速率限制
- **FR-006**: 所有下载操作 MUST 在 `data_update_logs` 表记录 source/status/record_count
- **FR-007**: `tradable_candidates.py` MUST 支持 `preserve_manual=True` 保留手动添加的 ticker
- **FR-008**: Tiingo API Key MUST 从 `secrets.env` 读取，不可硬编码在代码中

### Key Entities

- **PriceRecord**: Tiingo EOD 返回的单行数据（date, open, high, low, close, volume, adjClose, adjVolume, divCash, splitFactor）
- **FundamentalRecord**: SEC EDGAR XBRL concept（metric_name, value, report_date, period_start, period_end, period_type）
- **TradableCandidate**: CSV 行（ticker, name, exchange, source）

---

## Success Criteria

### Measurable Outcomes

- **SC-001**: 对 1000 只股票的全量历史下载（2005–2026）在 2 小时内完成
- **SC-002**: 增量下载单日新价格（1000 只）应在 10 分钟内完成
- **SC-003**: 下载失败率 < 1%（不包含合理的"无数据"情况）
- **SC-004**: SEC EDGAR 下载不触发 429 错误（通过 0.25s 间隔保障）
- **SC-005**: 所有下载任务执行后，`data_update_logs` 有对应条目记录结果

---

## Assumptions

- Tiingo API 已购买 EOD 数据权限（Token 存于 secrets.env）
- SEC EDGAR 为公开免费 API，无需鉴权
- yfinance 用于元数据填充，不用于价格数据（避免数据一致性问题）
- 下载脚本在市场收盘后（东部时间 16:30 后）手动或计划任务触发
- 价格数据的前复权处理由 Tiingo 完成，本地不做复权计算
- 目前基本面数据下载已实现但尚未与因子计算层打通
