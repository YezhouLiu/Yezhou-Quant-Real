# Feature Specification: 数据库 Schema 与读写层（Database Schema & Read-Write Layer）

**Feature Branch**: `001-database-schema`  
**Created**: 2026-04-21  
**Status**: ✅ Implemented  
**Input**: 慢量化系统的核心数据存储需求

---

## User Scenarios & Testing

### User Story 1 - 资产主数据管理（Instruments CRUD）(Priority: P1)

系统需要维护一份覆盖所有历史和当前可交易 US 股票/ETF 的主数据表，支持新增、查询和更新。

**Why this priority**: instruments 表是所有下游表的外键基础，其他任何数据都依赖它存在。

**Independent Test**: 通过 `rw_instruments.py` 插入一批 ticker，然后查询 `get_tradable_instrument_ids()`，返回列表中包含刚插入且 `is_tradable=True` 的 ticker。

**Acceptance Scenarios**:

1. **Given** 数据库为空，**When** 调用 `batch_insert_instruments([{ticker: 'AAPL', exchange: 'NASDAQ', asset_type: 'Stock'}])`, **Then** instruments 表有记录，`get_instrument_id('AAPL', 'NASDAQ')` 返回合法 ID
2. **Given** AAPL 已存在，**When** 再次 insert 相同 ticker+exchange，**Then** 系统 upsert 而非抛 unique 异常
3. **Given** 多条 instruments 存在，**When** 调用 `get_tradable_instrument_ids()`，**Then** 仅返回 `is_tradable=True` 的记录

---

### User Story 2 - 历史价格读写（Market Prices CRUD）(Priority: P1)

完整存储每只证券的日度 OHLCV + 前复权价格，支持批量写入和按日期/证券过滤查询。

**Why this priority**: 价格数据是因子计算和回测的直接输入，缺少价格则一切下游都无法运行。

**Independent Test**: 批量写入 100 条 AAPL 价格，调用 `get_prices(instrument_id, '2020-01-01', '2020-12-31')` 返回完整 100 行。

**Acceptance Scenarios**:

1. **Given** 已有 instrument_id=1 的 AAPL，**When** `batch_insert_prices(rows)` 写入 252 条 2020 年 records，**Then** `get_price_max_date(1)` 返回 2020-12-31
2. **Given** 价格数据已存在，**When** 重复 insert 同日 same instrument，**Then** 通过 ON CONFLICT DO UPDATE 更新而非报错
3. **Given** 查询某日所有股票价格，**When** `get_prices_on_date('2024-01-15')`, **Then** 返回该日有数据的所有 instrument

---

### User Story 3 - 因子值版本化存储（Factor Values）(Priority: P1)

以 `(instrument_id, date, factor_name, factor_version)` 为复合主键存储因子值，支持多版本共存和批量写入。

**Why this priority**: 因子存储是整个量化管道的产出仓库，选股和回测都从这里读取信号。

**Independent Test**: 对同一 factor_name 用不同 window 参数分别 insert，两个版本都能独立查询且不互相覆盖。

**Acceptance Scenarios**:

1. **Given** 某日所有 instrument 的 momentum 值已计算，**When** `batch_insert_factor_values(rows)`，**Then** `get_factor_snapshot(date='2024-01-31', factor_name='mom_63d')` 返回同日所有股票的该因子值
2. **Given** 同一因子有 v1 和 v2 两个版本，**When** 各自查询，**Then** 两个版本独立返回，无混淆
3. **Given** 旧版本数据已存在，**When** 重新计算后 upsert，**Then** factor_value 更新，其余字段不变

---

### User Story 4 - 回测持仓快照（Exp Positions）(Priority: P2)

回测引擎每日输出持仓快照（含现金占位），可聚合为 NAV 时间序列。

**Why this priority**: 回测结果持久化是回测分析和可视化的前提。

**Independent Test**: 运行一段回测后，调用 `get_exp_nav(conn)` 返回按日期升序的 (date, nav) 序列，NAV = 所有头寸 market_value 之和。

**Acceptance Scenarios**:

1. **Given** 某月末再平衡完成，**When** `batch_insert_exp_positions(snapshots)`，**Then** 包含 CASH(`instrument_id=0`) 和各股票的行
2. **Given** 多个月的快照已存储，**When** `get_exp_nav(conn)`，**Then** 返回每日 SUM(market_value) 的时间序列
3. **Given** 需要区分股票和现金，**When** `get_stock_positions_only(conn, date)` / `get_cash_only(conn, date)`，**Then** 正确分类

---

### Edge Cases

- 写入 `instrument_id` 不存在于 `instruments` 表的价格/因子 → 外键约束应抛异常
- `factor_value` 为 NULL 或 NaN → 不写入（调用方过滤后再 insert）
- 数据库连接失败 → `db_utils.get_db_connection()` 抛出可识别异常

---

## Requirements

### Functional Requirements

- **FR-001**: 系统 MUST 提供 13 张表的完整 DDL，通过 `create_tables.py` 一键创建
- **FR-002**: 系统 MUST 支持 `instruments` 表的批量 upsert（ON CONFLICT DO UPDATE）
- **FR-003**: 系统 MUST 支持 `market_prices` 按 `(instrument_id, date)` 范围查询
- **FR-004**: 系统 MUST 支持 `factor_values` 按 `(factor_name, factor_version, date)` 的 snapshot 查询
- **FR-005**: 系统 MUST 通过 `system_state` 表跟踪每个数据源的最后更新时间
- **FR-006**: 系统 MUST 通过 `data_update_logs` 表记录每次下载的结果（成功/失败/条数）
- **FR-007**: `exp_positions` 表 MUST 用 `instrument_id=0` 表示现金持仓
- **FR-008**: 所有 rw_* 模块 MUST 接受 `conn`（psycopg2 connection）作为第一参数，不自行管理连接

### Key Entities

- **Instrument**: 证券主数据（ticker, exchange, asset_type, is_tradable, sector, industry）
- **MarketPrice**: OHLCV + adj_close + dividends + splits，以 (instrument_id, date) 为键
- **FactorValue**: 因子快照，以 (instrument_id, date, factor_name, factor_version) 为键
- **ExpPosition**: 回测持仓快照，以 (date, instrument_id) 为键，包含现金（id=0）
- **SystemState**: KV 存储格式（state_key, state_value），用于记录增量下载断点

---

## Success Criteria

### Measurable Outcomes

- **SC-001**: 所有 13 张表可通过 `python database/schema/create_tables.py` 一键创建，执行耗时 < 5 秒
- **SC-002**: `batch_insert_prices()` 写入 10,000 条价格记录的耗时 < 10 秒
- **SC-003**: `get_factor_snapshot(date, factor_name)` 在 50 万条因子记录时查询耗时 < 2 秒
- **SC-004**: `get_exp_nav()` 在 5 年回测数据（约 1,250 日 × 10 持仓）下耗时 < 1 秒

---

## Assumptions

- 数据库为本地 PostgreSQL 实例（localhost:5432），生产用途不涉及云数据库
- psycopg2 连接由调用方传入，rw 模块不负责连接池管理
- `instrument_id=0` 永久预留为现金，不会出现在 `instruments` 表中
- `market_prices` 中的 `adj_close` 已是前复权，调整逻辑由 Tiingo API 处理
- 因子计算结果在写入前由调用方过滤掉 NaN，DB 层不做数据清洗
