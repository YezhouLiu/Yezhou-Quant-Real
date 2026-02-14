# Yezhou 量化交易系统

## 📋 项目概述

这是一个基于 PostgreSQL 的美股量化交易系统，用于因子研究、策略回测和实盘交易。系统设计遵循模块化原则，支持多因子模型开发和日线级别的股票筛选。

**核心功能**：
- 美股标的池管理（S&P 500、NASDAQ 100等）
- Tiingo EOD价格数据下载与存储
- 因子计算引擎（动量、波动率、跳空风险、最大回撤、美元成交量等）
- 交易日历与企业行为处理
- 回测与实盘持仓管理

---

## 🏗️ 代码结构

```
Yezhou-Quant-Real/
│
├── main.py                      # 主入口：daily_update() 日常任务
├── config/
│   └── config.yaml              # 全局配置（数据库、交易参数、日志）
│
├── database/                    # 数据库层 ⭐ 核心
│   ├── schema/                  # 表结构定义
│   │   ├── create_tables.py     # 一键建表脚本
│   │   └── tables/              # 各表DDL（11张表）
│   ├── readwrite/               # RW方法（数据存取接口）
│   │   ├── rw_instruments.py    # 资产主表
│   │   ├── rw_market_prices.py  # 价格数据
│   │   ├── rw_factor_values.py  # 因子值
│   │   └── ...                  # 其他表的RW方法
│   └── utils/
│       └── db_utils.py          # 数据库连接工具
│
├── data_download/               # 数据获取
│   ├── input/                   # 初始化数据
│   │   ├── price_downloader.py  # Tiingo价格下载器
│   │   ├── all_us_stocks.py     # 全市场股票列表
│   │   └── ...
│   └── update/                  # 增量更新
│       ├── update_tradable_universe.py  # 每日更新标的池
│       └── fill_sector_industry_yfinance.py
│
├── factors/                     # 因子定义库
│   ├── momentum.py              # 动量因子计算
│   ├── volatility.py            # 波动率因子
│   ├── volatility_of_volatility.py  # 波动率的波动率
│   ├── dollar_volume.py         # 美元成交量因子
│   ├── jump_risk.py             # 跳空风险因子
│   └── max_drawdown.py          # 最大回撤因子
│
├── engine/                      # 计算引擎
│   └── compute_factors/
│       ├── compute_momentum.py  # 因子批量计算入口
│       ├── compute_volatility.py
│       ├── compute_volatility_of_volatility.py
│       ├── compute_dollar_volume.py
│       ├── compute_jump_risk.py
│       └── compute_max_drawdown.py
│
├── tasks/                       # 定时任务
│   ├── daily_tasks.py           # 每日：下载价格、更新标的、提取企业行为
│   ├── seasonal_tasks.py        # 季度：基本面数据
│   └── annual_tasks.py          # 年度：深度清洗
│
├── tests/                       # 单元测试
│   ├── database/                # 数据库RW方法测试
│   ├── factors/                 # 因子计算测试
│   └── ...
│
└── utils/                       # 工具函数
    ├── logger.py                # 日志系统
    ├── config_loader.py         # 配置加载器
    └── time.py                  # 日期工具
```

---

## �️ 数据库架构

### 数据表总览（11张表）

系统采用 PostgreSQL 作为核心数据库，所有表通过 `instrument_id` 作为统一外键关联。

#### 核心数据表
1. **instruments** - 资产主表
2. **instrument_identifiers** - 跨数据源映射表
3. **market_prices** - 市场价格数据（OHLCV + 复权）
4. **corporate_actions** - 企业行为（分红、拆股）
5. **fundamental_data** - 基本面数据（预留）

#### 因子与回测表
6. **factor_values** - 因子值表
7. **trading_calendar** - 交易日历表

#### 交易与持仓表
8. **fills** - 成交记录表
9. **positions** - 持仓快照表

#### 系统管理表
10. **system_state** - 系统状态/配置表
11. **data_update_logs** - 数据更新日志表

**标的池管理**：系统使用 `instruments.is_tradable` 字段直接标记可交易资产，通过 `update_tradable_universe()` 基于市场数据（价格、成交量）动态更新。初始候选池通过 CSV 文件管理（`csv/tradable_candidates.csv`），支持从 Russell 1000/2000、S&P 500 等指数爬取。

---

### 📊 详细表结构与 I/O 方法

#### 1. instruments（资产主表）

**用途**：统一管理所有交易资产（Stock/ETF/Cash），通过稳定的 `instrument_id` 解决 ticker 改名问题

**表结构**：
```sql
CREATE TABLE instruments (
    instrument_id BIGSERIAL PRIMARY KEY,          -- 稳定主键
    ticker TEXT NOT NULL,                         -- 股票代码
    exchange TEXT NOT NULL DEFAULT 'US',          -- 交易所
    asset_type TEXT NOT NULL DEFAULT 'Stock',     -- 资产类型：Stock/ETF/Cash
    currency TEXT NOT NULL DEFAULT 'USD',         -- 货币
    
    company_name TEXT,                            -- 公司名称
    description TEXT,                             -- 描述
    sector TEXT,                                  -- 行业分类
    industry TEXT,                                -- 子行业
    ipo_date DATE,                                -- 上市日期
    delist_date DATE,                             -- 退市日期
    
    status TEXT NOT NULL DEFAULT 'active',        -- 状态：active/delisted/suspended/bankrupt
    is_tradable BOOLEAN DEFAULT FALSE,            -- 是否在交易池中
    is_factor_enabled BOOLEAN DEFAULT FALSE,      -- 是否启用因子计算
    
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    
    UNIQUE(ticker, exchange)
);
```

**索引**：
- `idx_instruments_ticker`
- `idx_instruments_ticker_exchange`
- `idx_instruments_tradable`
- `idx_instruments_status`

**I/O 方法**（`database/readwrite/rw_instruments.py`）：
- `insert_instrument(conn, ticker, exchange, ...)` → int: 插入或更新资产，返回 instrument_id
- `get_instrument_id(conn, ticker, exchange)` → Optional[int]: 根据 ticker 获取 ID
- `get_instrument_by_id(conn, instrument_id)` → Optional[Dict]: 根据 ID 获取资产信息
- `get_all_instruments(conn, asset_type, is_tradable)` → pd.DataFrame: 获取资产列表
- `update_instrument_tradable(conn, instrument_id, is_tradable)`: 更新可交易状态

---

#### 2. instrument_identifiers（跨数据源映射表）

**用途**：管理不同数据源的标识符映射（Tiingo/YFinance/CUSIP/ISIN）

**表结构**：
```sql
CREATE TABLE instrument_identifiers (
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    id_type TEXT NOT NULL,                        -- 标识符类型：tiingo/yfinance/cusip/isin/sedol/figi
    id_value TEXT NOT NULL,                       -- 标识符值
    valid_from DATE,                              -- 有效期开始
    valid_to DATE,                                -- 有效期结束
    created_at TIMESTAMPTZ DEFAULT now(),
    
    PRIMARY KEY (id_type, id_value, instrument_id)
);
```

**I/O 方法**（`database/readwrite/rw_instrument_identifiers.py`）：
- `insert_identifier(conn, instrument_id, id_type, id_value, ...)`
- `get_instrument_by_identifier(conn, id_type, id_value)` → Optional[int]

---

#### 3. market_prices（市场价格表）

**用途**：存储 Tiingo EOD 价格数据，包含完整 OHLCV 和复权后的价格

**表结构**：
```sql
CREATE TABLE market_prices (
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    date DATE NOT NULL,
    
    open_price NUMERIC(20,6),                     -- 开盘价
    high_price NUMERIC(20,6),                     -- 最高价
    low_price NUMERIC(20,6),                      -- 最低价
    close_price NUMERIC(20,6) NOT NULL,           -- 收盘价
    volume BIGINT,                                -- 成交量
    
    adj_open NUMERIC(20,6),                       -- 复权开盘价
    adj_high NUMERIC(20,6),                       -- 复权最高价
    adj_low NUMERIC(20,6),                        -- 复权最低价
    adj_close NUMERIC(20,6) NOT NULL,             -- 复权收盘价
    adj_volume BIGINT,                            -- 复权成交量
    
    dividends NUMERIC(20,6) DEFAULT 0,            -- 当日分红（美元）
    stock_splits NUMERIC(20,6) DEFAULT 1,         -- 拆股因子（2.0=1拆2，0.5=2合1）
    
    data_source TEXT NOT NULL DEFAULT 'tiingo',
    ingested_at TIMESTAMPTZ DEFAULT now(),
    
    PRIMARY KEY (instrument_id, date)
);
```

**索引**：
- `idx_prices_date`
- `idx_prices_instrument_date`

**I/O 方法**（`database/readwrite/rw_market_prices.py`）：
- `insert_price(conn, instrument_id, date, close_price, adj_close, ...)`
- `batch_insert_prices(conn, prices: List[Dict])`
- `get_prices(conn, instrument_id, start_date, end_date)` → pd.DataFrame
- `get_latest_price(conn, instrument_id)` → Optional[Dict]
- `get_price_on_date(conn, instrument_id, date)` → Optional[Dict]
- `delete_prices(conn, instrument_id, start_date, end_date)`
- `get_price_max_date(conn)` → Optional[str]: 获取数据库中最新的价格日期

---

#### 4. corporate_actions（企业行为表）

**用途**：记录分红、拆股、特殊股息等企业行为

**表结构**：
```sql
CREATE TABLE corporate_actions (
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    action_date DATE NOT NULL,
    action_type TEXT NOT NULL,                    -- DIVIDEND_CASH/SPLIT/REVERSE_SPLIT/SPINOFF/SPECIAL_DIVIDEND
    
    action_value NUMERIC(20,6),                   -- 分红金额（每股）或拆股比例
    currency TEXT DEFAULT 'USD',
    
    data_source TEXT DEFAULT 'tiingo',
    raw_payload JSONB,                            -- Tiingo 原始返回数据
    ingested_at TIMESTAMPTZ DEFAULT now(),
    
    PRIMARY KEY (instrument_id, action_date, action_type)
);
```

**索引**：
- `idx_corp_action_instrument_date`
- `idx_corp_action_type`

**I/O 方法**（`database/readwrite/rw_corporate_actions.py`）：
- `insert_corporate_action(conn, instrument_id, action_date, action_type, action_value, ...)`
- `batch_insert_corporate_actions(conn, actions: List[Dict])`
- `get_corporate_actions(conn, instrument_id, action_type, start_date, end_date)` → pd.DataFrame
- `get_latest_corporate_action_date(conn, instrument_id, action_type)` → Optional[str]
- `delete_corporate_actions(conn, instrument_id, start_date, end_date)`

---

#### 5. fundamental_data（基本面数据表）

**用途**：存储 SEC EDGAR 基本面数据（预留，暂未使用）

**表结构**：
```sql
CREATE TABLE fundamental_data (
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    report_date DATE NOT NULL,
    metric_name TEXT NOT NULL,                    -- EPS/Revenue/NetIncome/ROE/PE/PB...
    
    metric_value NUMERIC(38,10),
    period_type TEXT NOT NULL,                    -- TTM/Quarterly/Annual
    period_start DATE,
    period_end DATE,
    
    currency TEXT DEFAULT 'USD',
    data_source TEXT DEFAULT 'sec_edgar',
    ingested_at TIMESTAMPTZ DEFAULT now(),
    
    PRIMARY KEY (instrument_id, report_date, metric_name, period_type)
);
```

**索引**：
- `idx_fundamental_instrument_date`
- `idx_fundamental_metric`

**I/O 方法**（`database/readwrite/rw_fundamental_data.py`）：
- `insert_fundamental(conn, instrument_id, report_date, metric_name, metric_value, ...)`
- `get_fundamentals(conn, instrument_id, metric_name, start_date, end_date)` → pd.DataFrame

---

#### 6. factor_values（因子值表）

**用途**：存储所有因子的计算结果（动量、波动率、美元成交量、跳空风险等）

**表结构**：
```sql
CREATE TABLE factor_values (
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    date DATE NOT NULL,
    factor_name TEXT NOT NULL,                    -- 因子名称，如 'mom_252d_skip21', 'vol_60d'
    
    factor_value NUMERIC(38,10) NOT NULL,         -- 因子数值
    
    factor_args JSONB,                            -- 因子参数（lookback/skip/window/half_life 等）
    config JSONB,                                 -- 预处理配置（winsor/zscore/universe/price_field 等）
    
    factor_version TEXT NOT NULL DEFAULT 'v1',    -- 因子版本（用于重算与并存）
    
    data_source TEXT NOT NULL DEFAULT 'internal',
    ingested_at TIMESTAMPTZ DEFAULT now(),
    
    PRIMARY KEY (instrument_id, date, factor_name, factor_version)
);
```

**索引**：
- `idx_factor_values_name_date_ver`: 某因子某天的截面（选股/IC/分组）
- `idx_factor_values_instrument_date`: 单标的因子时间序列
- `idx_factor_values_date`: 某天的全部因子

**I/O 方法**（`database/readwrite/rw_factor_values.py`）：
- `insert_factor_value(conn, instrument_id, date, factor_name, factor_value, factor_version, factor_args, config, ...)`
- `batch_insert_factor_values(conn, rows: List[Dict])`
- `get_factor_values(conn, factor_name, factor_version, instrument_id, start_date, end_date)` → pd.DataFrame
- `get_latest_factor_value(conn, instrument_id, factor_name, factor_version)` → Optional[Dict]
- `get_factor_snapshot(conn, date, factor_name, factor_version)` → pd.DataFrame: 获取某天所有标的的某个因子值
- `delete_factor_values(conn, factor_name, factor_version, instrument_id, start_date, end_date)`

---

#### 7. trading_calendar（交易日历表）

**用途**：记录美股交易日历，标记交易日和节假日

**表结构**：
```sql
CREATE TABLE trading_calendar (
    market TEXT NOT NULL DEFAULT 'US',
    date DATE NOT NULL,
    is_trading_day BOOLEAN NOT NULL,              -- 是否交易日
    holiday_name TEXT,                            -- 节假日名称
    
    PRIMARY KEY (market, date)
);
```

**索引**：
- `idx_trading_calendar_date`

**I/O 方法**（`database/readwrite/rw_trading_calendar.py`）：
- `insert_trading_day(conn, date, is_trading_day, holiday_name, market='US')`
- `batch_insert_trading_days(conn, days: List[Dict])`
- `is_trading_day(conn, date, market='US')` → bool
- `get_trading_days(conn, start_date, end_date, market='US')` → pd.DataFrame
- `get_next_trading_day(conn, date, market='US')` → Optional[str]
- `get_prev_trading_day(conn, date, market='US')` → Optional[str]

---

#### 8. fills（成交记录表）

**用途**：记录所有买卖成交记录

**表结构**：
```sql
CREATE TABLE fills (
    fill_id BIGSERIAL PRIMARY KEY,
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    side TEXT NOT NULL,                           -- BUY/SELL
    
    quantity NUMERIC(20,8) NOT NULL,              -- 成交数量
    price NUMERIC(20,6) NOT NULL,                 -- 成交价格
    trade_time TIMESTAMPTZ NOT NULL,              -- 成交时间
    
    commission NUMERIC(20,6) DEFAULT 0,           -- 佣金
    fees NUMERIC(20,6) DEFAULT 0,                 -- 其他费用
    fx_rate NUMERIC(20,8),                        -- 汇率
    
    notes TEXT,
    source TEXT DEFAULT 'manual',                 -- manual/ibkr/csv_import/api
    created_at TIMESTAMPTZ DEFAULT now()
);
```

**索引**：
- `idx_fills_instrument_time`
- `idx_fills_trade_time`

**I/O 方法**（`database/readwrite/rw_fills.py`）：
- `insert_fill(conn, instrument_id, side, quantity, price, trade_time, commission, fees, ...)`
- `get_fills(conn, instrument_id, start_date, end_date)` → pd.DataFrame
- `get_fill_by_id(conn, fill_id)` → Optional[Dict]
- `delete_fill(conn, fill_id)`

---

#### 9. positions（持仓快照表）

**用途**：记录每日持仓快照

**表结构**：
```sql
CREATE TABLE positions (
    date DATE NOT NULL,
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    
    quantity NUMERIC(20,8) NOT NULL,              -- 持仓数量
    cost_basis NUMERIC(20,6),                     -- 成本基础
    last_price NUMERIC(20,6),                     -- 估值价格
    market_value NUMERIC(20,6),                   -- 市值
    
    updated_at TIMESTAMPTZ DEFAULT now(),
    source TEXT DEFAULT 'computed',               -- computed/manual_adjust
    
    PRIMARY KEY (date, instrument_id)
);
```

**索引**：
- `idx_positions_date`

**I/O 方法**（`database/readwrite/rw_positions.py`）：
- `insert_position(conn, date, instrument_id, quantity, cost_basis, last_price, market_value, ...)`
- `batch_insert_positions(conn, positions: List[Dict])`
- `get_positions(conn, date)` → pd.DataFrame
- `get_position_history(conn, instrument_id, start_date, end_date)` → pd.DataFrame
- `delete_positions(conn, date)`

---

#### 10. system_state（系统状态表）

**用途**：存储系统配置和状态（现金资产 ID、最后更新时间等）

**表结构**：
```sql
CREATE TABLE system_state (
    key TEXT PRIMARY KEY,                         -- 状态键
    value JSONB,                                  -- 状态值（JSON 格式）
    updated_at TIMESTAMPTZ DEFAULT now()
);
```

**I/O 方法**（`database/readwrite/rw_system_state.py`）：
- `set_state(conn, key, value: Dict)`
- `get_state(conn, key)` → Optional[Dict]
- `delete_state(conn, key)`

**常用状态键**：
- `cash_instrument_id`: 现金资产的 instrument_id
- `last_price_update`: 最后一次价格更新时间
- `last_universe_update`: 最后一次标的池更新时间

---

#### 11. data_update_logs（数据更新日志表）

**用途**：监控数据更新任务的执行情况（Tiingo API 调用、因子计算等）

**表结构**：
```sql
CREATE TABLE data_update_logs (
    log_id BIGSERIAL PRIMARY KEY,
    dataset TEXT NOT NULL,                        -- market_prices/fundamental_data/universe/instruments
    source TEXT NOT NULL,                         -- 数据源
    
    start_date DATE,                              -- 更新起始日期
    end_date DATE,                                -- 更新结束日期
    instruments_count INT,                        -- 标的数量
    rows_inserted INT,                            -- 插入行数
    rows_updated INT,                             -- 更新行数
    
    status TEXT NOT NULL DEFAULT 'running',       -- running/completed/failed/partial
    error_message TEXT,
    
    started_at TIMESTAMPTZ DEFAULT now(),
    completed_at TIMESTAMPTZ,
    duration_seconds INT
);
```

**索引**：
- `idx_update_logs_dataset`
- `idx_update_logs_status`

**I/O 方法**（`database/readwrite/rw_data_update_logs.py`）：
- `create_log(conn, dataset, source, start_date, end_date, instruments_count)` → int: 创建日志，返回 log_id
- `update_log_success(conn, log_id, rows_inserted, rows_updated)`: 更新日志为成功状态
- `update_log_failure(conn, log_id, error_message)`: 更新日志为失败状态
- `get_recent_logs(conn, dataset, limit)` → pd.DataFrame: 获取最近的日志记录

---

## �🔄 业务逻辑

### 1. 数据流水线

```
┌─────────────────┐
│  Tiingo API     │ 每日 EOD 价格（OHLCV + 复权）
└────────┬────────┘
         ↓
┌─────────────────┐
│ price_downloader│ 批量下载 → market_prices 表
└────────┬────────┘
         ↓
┌─────────────────┐
│ instruments     │ 标的主表（ticker → instrument_id）
│  .is_tradable   │ 动态标记可交易资产（基于价格/成交量）
└────────┬────────┘
         ↓
┌─────────────────┐
│ factor_values   │ 因子计算结果（动量、波动率、美元成交量、跳空风险等）
└────────┬────────┘
         ↓
┌─────────────────┐
│ 选股/回测       │ 根据因子排序选股 → positions
└─────────────────┘
```

### 2. 每日更新流程（`daily_tasks.py`）

```python
def daily_update():
    1. download_prices()              # 下载最新价格
    2. extract_corporate_actions()    # 提取分红、拆股
    3. update_tradable_universe()     # 更新可交易标的池
```

### 3. 因子计算流程

```python
# factors/momentum.py
calc_single_instrument_momentum(
    conn, instrument_id, start_date, end_date,
    lookback=252,  # 回溯252天
    skip=21        # 跳过最近21天
)
# 计算公式：(price_t0 / price_t1) - 1
# 其中：t0 = 当前日期 - skip，t1 = t0 - lookback
```

**因子命名规范**：`mom_252d_skip21` = 动量因子（252天回溯期，跳过21天）

---

### 4. 已实现因子库 📊

#### 4.1 动量因子（Momentum）
- **文件**：`factors/momentum.py`
- **因子名称**：`mom_{lookback}d_skip{skip}`
- **计算公式**：`(price_t-skip / price_t-skip-lookback) - 1`
- **默认参数**：`lookback=252`, `skip=21`
- **理论依据**：动量效应（Jegadeesh & Titman, 1993）
- **适用场景**：捕捉中期趋势，跳过近期反转

#### 4.2 波动率因子（Volatility）
- **文件**：`factors/volatility.py`
- **因子名称**：`vol_{window}d_ann{annualize}`
- **计算公式**：`std(daily_returns) * sqrt(annualize)`
- **默认参数**：`window=60`, `annualize=252`
- **理论依据**：低波动异象（Low-Volatility Anomaly）
- **适用场景**：风险调整、防守性策略

#### 4.3 波动率的波动率（Volatility of Volatility）
- **文件**：`factors/volatility_of_volatility.py`
- **因子名称**：`volvol_{volvol_window}d_from_vol{vol_window}d`
- **计算公式**：先计算滚动波动率序列，再计算波动率的标准差
- **默认参数**：`vol_window=20`, `volvol_window=60`
- **理论依据**：波动率风险溢价
- **适用场景**：识别不稳定、高风险资产

#### 4.4 美元成交量因子（Dollar Volume）
- **文件**：`factors/dollar_volume.py`
- **因子名称**：`dv_{window}d_log`
- **计算公式**：`log(mean(adj_close * adj_volume))`
- **默认参数**：`window=20`
- **理论依据**：流动性溢价
- **适用场景**：过滤流动性不足的小盘股

#### 4.5 跳空风险因子（Jump Risk）
- **文件**：`factors/jump_risk.py`
- **因子名称**：`jump_{window}d_max`, `jump_{window}d_cnt`
- **计算公式**：
  - `jump = abs((high - low) / close - 1)` 超过阈值的次数和最大值
  - `jump_threshold=0.95`, `jump_ratio_limit=10.0`
- **默认参数**：`window=60`
- **理论依据**：跳空风险（Tail Risk）
- **适用场景**：风险管理、事件驱动策略

#### 4.6 最大回撤因子（Maximum Drawdown）
- **文件**：`factors/max_drawdown.py`
- **因子名称**：`mdd_{window}d`
- **计算公式**：`(running_max - current_price) / running_max` 的最大值
- **默认参数**：`window=252`
- **理论依据**：下行风险度量
- **适用场景**：风险控制、尾部风险管理

**因子使用示例**：
```python
# 计算单标的的所有因子
from factors.momentum import calc_single_instrument_momentum
from factors.volatility import calc_single_instrument_volatility
from factors.dollar_volume import calc_single_instrument_dollar_volume

conn = get_db_connection()

# 动量因子
calc_single_instrument_momentum(conn, instrument_id=123, 
    start_date='2020-01-01', end_date='2024-12-31',
    lookback=252, skip=21, factor_version='v1')

# 波动率因子
calc_single_instrument_volatility(conn, instrument_id=123,
    start_date='2020-01-01', end_date='2024-12-31',
    window=60, annualize=252, factor_version='v1')

# 美元成交量因子
calc_single_instrument_dollar_volume(conn, instrument_id=123,
    start_date='2020-01-01', end_date='2024-12-31',
    window=20, factor_version='v1')

conn.commit()
```

---

## 🧪 因子开发指南

### 新因子开发流程

1. **在 `factors/` 目录下创建新文件**（例如 `volatility.py`）
2. **实现因子计算函数**：
   ```python
   def calc_single_instrument_volatility(
       conn, instrument_id, start_date, end_date,
       window=60, factor_version="v1"
   ):
       # 1. 从 market_prices 读取数据
       df = get_prices(conn, instrument_id, ...)
       
       # 2. 计算因子
       df['returns'] = df['adj_close'].pct_change()
       df['volatility'] = df['returns'].rolling(window).std() * np.sqrt(252)
       
       # 3. 构造写入数据
       rows = [{
           'instrument_id': instrument_id,
           'date': row['date'],
           'factor_name': f'vol_{window}d',
           'factor_value': row['volatility'],
           'factor_args': {'window': window},
           'factor_version': factor_version
       }]
       
       # 4. 批量写入 factor_values
       batch_insert_factor_values(conn, rows)
   ```

3. **在 `engine/compute_factors/` 创建批量计算脚本**：
   ```python
   def compute_volatility():
       conn = get_db_connection()
       instruments = get_all_tradable_instruments(conn)
       
       for inst_id in instruments['instrument_id']:
           calc_single_instrument_volatility(
               conn, inst_id, 
               start_date='2020-01-01', 
               end_date='2024-12-31'
           )
       conn.commit()
   ```

4. **添加单元测试**（`tests/factors/test_volatility.py`）

5. **在 `main.py` 中调用**（可选，加入定时任务）

---

## 📊 常用 SQL 查询示例

### 1. 获取某天的因子截面数据（用于选股）

```sql
SELECT 
    i.ticker,
    i.sector,
    fv.factor_value as momentum,
    mp.adj_close as price,
    mp.adj_volume as volume
FROM factor_values fv
JOIN instruments i ON fv.instrument_id = i.instrument_id
LEFT JOIN market_prices mp ON fv.instrument_id = mp.instrument_id 
    AND fv.date = mp.date
WHERE fv.factor_name = 'mom_252d_skip21'
  AND fv.date = '2024-01-15'
  AND fv.factor_version = 'v1'
  AND i.is_tradable = TRUE
ORDER BY fv.factor_value DESC
LIMIT 50;
```

### 2. 计算因子 IC（信息系数）

```sql
-- 需要在应用层用 Pandas 计算
-- 1. 取 t 日因子值
-- 2. 取 t+20 日收益率
-- 3. 计算相关系数
```

### 3. 查询可交易标的列表

```sql
SELECT 
    instrument_id,
    ticker,
    company_name,
    sector,
    industry
FROM instruments
WHERE is_tradable = TRUE
  AND asset_type = 'Stock'
ORDER BY ticker;
```

### 4. 查看数据更新日志

```sql
SELECT 
    job_name,
    start_time,
    end_time,
    status,
    rows_affected,
    error_message
FROM data_update_logs
ORDER BY start_time DESC
LIMIT 20;
```

---

## ⚙️ 配置说明（config.yaml）

```yaml
database:
  type: postgresql
  host: localhost
  port: 5432
  dbname: quant
  user: YezhouLiu

data:
  source: tiingo                         # 数据源
  default_start_date: "2005-01-01"       # 默认回测起始日期
  default_end_date: "2100-01-01"         # 默认结束日期

runtime:
  verbose: true                          # 详细日志
  dry_run: false                         # 是否模拟运行

backtest:
  capital: 100000                        # 初始资金
  default_backtest_start_date: "2005-01-01"
  default_backtest_end_date: "2100-01-01"

exchange:
  slippage: 0.005                        # 滑点（0.5%）
  transaction_cost: 0.001                # 交易成本（0.1%）
  exchange_cost: 0.0005                  # 交易所费用（0.05%）
  min_diff_buy_sell_ratio: 0.02          # 最小买卖差价比例（2%）
  rebalance_total_value_reinvest_ratio: 0.98  # 再投资比例（98%）

log:
  log_dir: logs                          # 日志目录
  log_level: INFO                        # 日志级别

path:
  csv_dir: csv                           # CSV输出目录

price:
  price_floor: 1.5                       # 最低价格（过滤低价股）
  price_ceiling: 10000.0                 # 最高价格
  jump_threshold: 0.95                   # 涨跌幅阈值（跳空检测）
  jump_ratio_limit: 10.0                 # 最大跳空比例
```

---

## 🚀 快速启动

### 1. 初始化数据库

```bash
# 创建所有表
python database/schema/create_tables.py

# 或使用批处理文件（Windows）
create_tables.bat
```

### 2. 下载初始数据

```bash
# 下载交易日历
python data_download/input/build_trading_calendar.py

# 下载全市场股票列表
python data_download/input/all_us_stocks.py

# 生成可交易候选池
python data_download/input/tradable_candidates.py

# 下载价格数据
python data_download/input/price_downloader.py
```

### 3. 计算因子

```bash
# 计算各类因子
python engine/compute_factors/compute_momentum.py
python engine/compute_factors/compute_volatility.py
python engine/compute_factors/compute_volatility_of_volatility.py
python engine/compute_factors/compute_dollar_volume.py
python engine/compute_factors/compute_jump_risk.py
python engine/compute_factors/compute_max_drawdown.py
```

### 4. 每日更新

```bash
# 运行每日任务
python main.py
```

---

## 📝 TODO / 下一步计划

- [x] 添加更多因子（波动率、成交量、反转、跳空风险、最大回撤）✅
- [ ] 实现因子合成（线性加权、机器学习）
- [ ] 回测引擎优化（支持多空策略）
- [ ] 实盘交易接口（Interactive Brokers）
- [ ] 风险管理模块（VaR、最大回撤限制）
- [ ] 可视化面板（因子IC、持仓分布、收益曲线）
- [ ] 因子有效性分析（IC、分组回测、因子相关性）
- [ ] 风险管理模块（VaR、最大回撤限制）
- [ ] 可视化面板（因子IC、持仓分布、收益曲线）

---

## 📞 联系方式

- **作者**：Yezhou Liu
- **邮箱**：YezhouLiu7@gmail.com
- **数据库**：PostgreSQL @ localhost:5432/quant

---

**最后更新**：2026-02-08
