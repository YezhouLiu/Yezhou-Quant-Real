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
│   │   └── tables/              # 各表DDL（14张表）
│   ├── readwrite/               # RW方法（数据存取接口）
│   │   ├── rw_instruments.py    # 资产主表
│   │   ├── rw_market_prices.py  # 价格数据
│   │   ├── rw_factor_values.py  # 因子值
│   │   ├── rw_universe_*.py     # 标的池管理
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

## 🔄 业务逻辑

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
└────────┬────────┘
         ↓
┌─────────────────┐
│ universe_members│ 标的池成员（可交易股票池）
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

## 🗄️ 数据库结构（PostgreSQL）⭐ 最关键

### 表结构总览（14张表）

| 表名 | 作用 | 主键 |
|------|------|------|
| `instruments` | 资产主表（Stock/ETF/Cash） | `instrument_id` |
| `instrument_identifiers` | 多标识符映射（CUSIP/ISIN/FIGI） | `(instrument_id, id_type)` |
| `market_prices` | Tiingo EOD价格（OHLCV+复权） | `(instrument_id, date)` |
| `fundamental_data` | 基本面数据（预留，暂未使用） | `(instrument_id, report_date, metric_name, period_type)` |
| `universe_definitions` | 标的池定义（SP500/NASDAQ100） | `universe_id` |
| `universe_snapshots` | 标的池快照（每日成员） | `(universe_id, snapshot_date)` |
| `universe_members` | 标的池成员列表 | `(universe_id, instrument_id, valid_from)` |
| `trading_calendar` | 交易日历 | `(market, date)` |
| `corporate_actions` | 企业行为（分红/拆股） | `(instrument_id, ex_date, action_type)` |
| `factor_values` | 因子值存储 | `(instrument_id, date, factor_name, factor_version)` |
| `fills` | 成交记录 | `fill_id` |
| `positions` | 持仓快照 | `(date, instrument_id)` |
| `system_state` | 系统状态（当前日期等） | `state_key` |
| `data_update_logs` | 数据更新日志 | `log_id` |

---

### 核心表详细结构

#### 1️⃣ `instruments` - 资产主表

```sql
CREATE TABLE instruments (
    instrument_id BIGSERIAL PRIMARY KEY,
    
    -- 标识信息
    ticker TEXT NOT NULL,
    exchange TEXT NOT NULL DEFAULT 'US',
    asset_type TEXT NOT NULL DEFAULT 'Stock',  -- Stock/ETF/Cash
    currency TEXT NOT NULL DEFAULT 'USD',
    
    -- 元数据
    company_name TEXT,
    description TEXT,
    sector TEXT,           -- GICS Sector
    industry TEXT,         -- GICS Industry
    ipo_date DATE,
    delist_date DATE,
    
    -- 状态标记
    status TEXT NOT NULL DEFAULT 'active',     -- active/delisted/suspended/bankrupt
    is_tradable BOOLEAN DEFAULT FALSE,         -- 是否在交易池中
    is_factor_enabled BOOLEAN DEFAULT FALSE,   -- 是否参与因子计算
    
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    
    UNIQUE(ticker, exchange)
);
```

**设计要点**：
- `instrument_id` 是稳定主键，解决 ticker 改名问题
- `is_tradable` 从 universe_members 同步，用于快速筛选
- `sector/industry` 用于行业中性化

---

#### 2️⃣ `market_prices` - 市场价格

```sql
CREATE TABLE market_prices (
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    date DATE NOT NULL,
    
    -- 原始价格（未复权）
    open_price NUMERIC(20,6),
    high_price NUMERIC(20,6),
    low_price NUMERIC(20,6),
    close_price NUMERIC(20,6) NOT NULL,
    volume BIGINT,
    
    -- 复权价格（向后复权）
    adj_open NUMERIC(20,6),
    adj_high NUMERIC(20,6),
    adj_low NUMERIC(20,6),
    adj_close NUMERIC(20,6) NOT NULL,
    adj_volume BIGINT,
    
    -- 企业行为
    dividends NUMERIC(20,6) DEFAULT 0,        -- 当日分红（美元）
    stock_splits NUMERIC(20,6) DEFAULT 1,     -- 拆股因子（2.0=1拆2, 0.5=2合1）
    
    data_source TEXT NOT NULL DEFAULT 'tiingo',
    ingested_at TIMESTAMPTZ DEFAULT now(),
    
    PRIMARY KEY (instrument_id, date)
);
```

**索引**：
- `idx_prices_date` - 按日期查询（截面数据）
- `idx_prices_instrument_date` - 单标的时间序列

---

#### 3️⃣ `factor_values` - 因子值存储 ⭐

```sql
CREATE TABLE factor_values (
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    date DATE NOT NULL,
    factor_name TEXT NOT NULL,                -- mom_252d_skip21, vol_60d, adv_20d
    
    factor_value NUMERIC(38,10) NOT NULL,     -- 因子标量值
    
    -- 因子参数（lookback, skip, window, half_life等）
    factor_args JSONB,
    
    -- 预处理配置（winsor, zscore, universe, price_field等）
    config JSONB,
    
    -- 因子版本（v1, v2, ...）用于重算与并存
    factor_version TEXT NOT NULL DEFAULT 'v1',
    
    data_source TEXT NOT NULL DEFAULT 'internal',
    ingested_at TIMESTAMPTZ DEFAULT now(),
    
    PRIMARY KEY (instrument_id, date, factor_name, factor_version)
);
```

**设计要点**：
- 一行 = 一个标的 × 一天 × 一个因子 × 一个版本 → 一个数值
- `factor_args` 示例：`{"lookback": 252, "skip": 21}`
- `config` 示例：`{"winsor": [0.01, 0.99], "zscore": true, "universe": "sp500"}`
- 支持因子版本并存，方便 A/B 测试

**索引**：
```sql
-- 某因子某天的截面（选股/IC/分组）
idx_factor_values_name_date_ver ON (factor_name, date, factor_version)

-- 单标的因子时间序列
idx_factor_values_instrument_date ON (instrument_id, date)

-- 某天取全部因子（构建训练集/回测）
idx_factor_values_date ON (date)
```

---

#### 4️⃣ `universe_definitions` - 标的池定义

```sql
CREATE TABLE universe_definitions (
    universe_id SERIAL PRIMARY KEY,
    universe_key TEXT NOT NULL UNIQUE,     -- 'sp500', 'nasdaq100', 'custom_tech'
    display_name TEXT NOT NULL,            -- 'S&P 500', 'NASDAQ 100'
    source_type TEXT NOT NULL,             -- wikipedia/manual/api/file_import
    source_ref TEXT,                       -- 数据源URL或文件路径
    created_at TIMESTAMPTZ DEFAULT now()
);
```

---

#### 5️⃣ `universe_members` - 标的池成员

```sql
CREATE TABLE universe_members (
    universe_id INT NOT NULL REFERENCES universe_definitions(universe_id) ON DELETE CASCADE,
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    
    valid_from DATE NOT NULL,              -- 生效日期
    valid_to DATE DEFAULT '2100-01-01',    -- 失效日期（9999=永久有效）
    
    reason TEXT,                           -- 加入/移除原因
    ingested_at TIMESTAMPTZ DEFAULT now(),
    
    PRIMARY KEY (universe_id, instrument_id, valid_from)
);
```

**使用方式**：
```sql
-- 查询某天的可交易标的
SELECT instrument_id FROM universe_members
WHERE universe_id = 1
  AND valid_from <= '2024-01-15'
  AND valid_to > '2024-01-15';
```

---

#### 6️⃣ `trading_calendar` - 交易日历

```sql
CREATE TABLE trading_calendar (
    market TEXT NOT NULL DEFAULT 'US',
    date DATE NOT NULL,
    is_trading_day BOOLEAN NOT NULL,
    holiday_name TEXT,
    
    PRIMARY KEY (market, date)
);
```

**数据来源**：`pandas_market_calendars` 库

---

#### 7️⃣ `corporate_actions` - 企业行为

```sql
CREATE TABLE corporate_actions (
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    ex_date DATE NOT NULL,
    action_type TEXT NOT NULL,             -- dividend/split/merger/spinoff
    
    amount NUMERIC(20,6),                  -- 分红金额或拆股比例
    currency TEXT DEFAULT 'USD',
    
    declaration_date DATE,
    record_date DATE,
    payment_date DATE,
    
    data_source TEXT DEFAULT 'tiingo',
    ingested_at TIMESTAMPTZ DEFAULT now(),
    
    PRIMARY KEY (instrument_id, ex_date, action_type)
);
```

**用途**：
- 复权价格验证
- 分红再投资策略
- 拆股事件过滤

---

#### 8️⃣ `positions` - 持仓快照

```sql
CREATE TABLE positions (
    date DATE NOT NULL,
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    
    quantity NUMERIC(20,8) NOT NULL,       -- 持仓数量
    cost_basis NUMERIC(20,6),              -- 成本价
    last_price NUMERIC(20,6),              -- 估值价格
    market_value NUMERIC(20,6),            -- 市值
    
    updated_at TIMESTAMPTZ DEFAULT now(),
    source TEXT DEFAULT 'computed',        -- computed/manual_adjust
    
    PRIMARY KEY (date, instrument_id)
);
```

---

#### 9️⃣ `fills` - 成交记录

```sql
CREATE TABLE fills (
    fill_id BIGSERIAL PRIMARY KEY,
    
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    trade_date DATE NOT NULL,
    
    side TEXT NOT NULL,                    -- buy/sell
    quantity NUMERIC(20,8) NOT NULL,
    price NUMERIC(20,6) NOT NULL,
    
    commission NUMERIC(20,6) DEFAULT 0,
    slippage NUMERIC(20,6) DEFAULT 0,
    
    order_type TEXT DEFAULT 'market',      -- market/limit/stop
    strategy_name TEXT,
    
    created_at TIMESTAMPTZ DEFAULT now()
);
```

---

#### 🔟 `data_update_logs` - 数据更新日志

```sql
CREATE TABLE data_update_logs (
    log_id BIGSERIAL PRIMARY KEY,
    
    job_name TEXT NOT NULL,                -- price_download, universe_update
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    
    status TEXT NOT NULL,                  -- success/failed/running
    rows_affected INT,
    error_message TEXT,
    
    metadata JSONB
);
```

---

## 🔌 必要的 RW 方法

### 1. `rw_instruments.py` - 资产管理

```python
# 插入/更新资产
insert_instrument(
    conn, ticker="AAPL", exchange="US", 
    company_name="Apple Inc.", sector="Technology"
) -> int  # 返回 instrument_id

# 根据 ticker 获取 ID
get_instrument_id(conn, ticker="AAPL", exchange="US") -> int

# 根据 ID 获取资产信息
get_instrument_by_id(conn, instrument_id=123) -> dict

# 批量获取所有可交易资产
get_all_tradable_instruments(conn) -> pd.DataFrame

# 更新行业信息
update_instrument_sector_industry(
    conn, instrument_id=123, 
    sector="Technology", industry="Consumer Electronics"
)

# 标记资产为可交易/不可交易
mark_tradable(conn, instrument_id=123, is_tradable=True)
```

---

### 2. `rw_market_prices.py` - 价格数据

```python
# 插入单条价格
insert_price(
    conn, instrument_id=123, date="2024-01-15",
    close_price=150.50, adj_close=145.20,
    volume=1000000, dividends=0, stock_splits=1
)

# 批量插入价格（高效）
batch_insert_prices(conn, prices: List[Dict])

# 获取价格数据
get_prices(
    conn, instrument_id=123,
    start_date="2024-01-01", end_date="2024-12-31"
) -> pd.DataFrame

# 获取最新价格
get_latest_price(conn, instrument_id=123) -> dict

# 获取某天所有股票的价格（截面数据）
get_cross_section_prices(conn, date="2024-01-15") -> pd.DataFrame
```

---

### 3. `rw_factor_values.py` - 因子值 ⭐

```python
# 插入单条因子值
insert_factor_value(
    conn,
    instrument_id=123,
    date="2024-01-15",
    factor_name="mom_252d_skip21",
    factor_value=0.15,
    factor_version="v1",
    factor_args={"lookback": 252, "skip": 21},
    config={"winsor": [0.01, 0.99]}
)

# 批量插入因子值（高效）
batch_insert_factor_values(conn, rows: List[Dict])

# 获取某因子某天的截面数据（用于选股）
get_factor_cross_section(
    conn, 
    factor_name="mom_252d_skip21", 
    date="2024-01-15",
    factor_version="v1"
) -> pd.DataFrame  # 列：instrument_id, ticker, factor_value

# 获取单标的多因子时间序列（用于回测）
get_factor_timeseries(
    conn, 
    instrument_id=123,
    start_date="2024-01-01", 
    end_date="2024-12-31"
) -> pd.DataFrame  # 列：date, mom_252d, vol_60d, adv_20d...

# 删除旧版本因子（重算时）
delete_factor_values(
    conn, 
    factor_name="mom_252d_skip21",
    factor_version="v1",
    start_date="2024-01-01", 
    end_date="2024-12-31"
)
```

---

### 4. `rw_universe.py` - 标的池管理

```python
# 创建标的池定义
create_universe_definition(
    conn, 
    universe_key="sp500", 
    display_name="S&P 500",
    source_type="wikipedia"
) -> int  # 返回 universe_id

# 批量添加标的池成员
batch_add_universe_members(
    conn,
    universe_id=1,
    instrument_ids=[123, 456, 789],
    valid_from="2024-01-01"
)

# 获取某天的标的池成员
get_universe_members_on_date(
    conn, 
    universe_key="sp500", 
    date="2024-01-15"
) -> List[int]  # 返回 instrument_id 列表

# 更新标的池快照（每日任务）
update_universe_snapshot(
    conn, 
    universe_id=1, 
    snapshot_date="2024-01-15",
    member_count=500
)
```

---

### 5. `rw_trading_calendar.py` - 交易日历

```python
# 批量插入交易日历
batch_insert_trading_days(conn, calendar_df: pd.DataFrame)

# 检查是否为交易日
is_trading_day(conn, date="2024-01-15", market="US") -> bool

# 获取下一个交易日
get_next_trading_day(conn, date="2024-01-15", market="US") -> str

# 获取日期范围内的所有交易日
get_trading_days(
    conn, 
    start_date="2024-01-01", 
    end_date="2024-12-31",
    market="US"
) -> List[str]
```

---

### 6. `rw_corporate_actions.py` - 企业行为

```python
# 插入企业行为
insert_corporate_action(
    conn,
    instrument_id=123,
    ex_date="2024-01-15",
    action_type="dividend",
    amount=0.50,
    record_date="2024-01-10",
    payment_date="2024-01-20"
)

# 获取某期间的企业行为
get_corporate_actions(
    conn,
    instrument_id=123,
    start_date="2024-01-01",
    end_date="2024-12-31"
) -> pd.DataFrame
```

---

### 7. `rw_positions.py` - 持仓管理

```python
# 更新持仓快照
upsert_position(
    conn,
    date="2024-01-15",
    instrument_id=123,
    quantity=100,
    cost_basis=150.0,
    last_price=155.0,
    market_value=15500.0
)

# 获取某天的持仓
get_positions_on_date(conn, date="2024-01-15") -> pd.DataFrame

# 计算持仓市值
calculate_portfolio_value(conn, date="2024-01-15") -> float
```

---

### 8. `rw_fills.py` - 成交记录

```python
# 记录成交
insert_fill(
    conn,
    instrument_id=123,
    trade_date="2024-01-15",
    side="buy",
    quantity=100,
    price=150.50,
    commission=1.0,
    slippage=0.05
)

# 获取某期间的成交记录
get_fills(
    conn,
    start_date="2024-01-01",
    end_date="2024-12-31"
) -> pd.DataFrame
```

---

## 🧪 因子开发指南

### 当前已实现的因子

1. **动量因子（Momentum）** - `factors/momentum.py`
   - `mom_252d_skip21`：252天动量，跳过最近21天
   - 计算公式：(price_t-21 / price_t-273) - 1
   - 用途：捕捉中期趋势，避免短期反转

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

### 3. 查询标的池历史成员变化

```sql
SELECT 
    i.ticker,
    um.valid_from,
    um.valid_to,
    um.reason
FROM universe_members um
JOIN instruments i ON um.instrument_id = i.instrument_id
WHERE um.universe_id = (SELECT universe_id FROM universe_definitions WHERE universe_key = 'sp500')
ORDER BY um.valid_from DESC;
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

## 🤖 给 ChatGPT 的因子建议提示词

```
我正在开发一个美股量化系统，数据库结构如下：

核心表：
1. instruments - 资产主表（ticker, sector, industry）
2. market_prices - 日线价格（OHLCV + 复权价格）
3. factor_values - 因子值（instrument_id, date, factor_name, factor_value）

已实现因子（共6个）：
1. mom_252d_skip21：动量因子（252天回溯，跳过21天）
   计算公式：(price_t-21 / price_t-273) - 1
   
2. vol_60d_ann252：波动率因子（60天窗口，年化252天）
   计算公式：std(daily_returns) * sqrt(252)
   
3. volvol_60d_from_vol20d：波动率的波动率
   计算公式：std(rolling_volatility_20d, window=60)
   
4. dv_20d_log：美元成交量因子（20天均值，取对数）
   计算公式：log(mean(adj_close * adj_volume))
   
5. jump_60d_max/cnt：跳空风险因子（60天窗口）
   计算公式：abs((high - low) / close - 1) 超过阈值的最大值和次数
   
6. mdd_252d：最大回撤因子（252天窗口）
   计算公式：max((running_max - price) / running_max)

数据特点：
- 标的池：S&P 500 成分股
- 频率：日线
- 数据源：Tiingo EOD
- 回测期：2005-至今

请基于以下原则建议 3-5 个新因子：
1. 能用 market_prices 表直接计算（无需基本面数据）
2. 与已有因子低相关（避免冗余）
3. 有学术研究支持或实践验证
4. 计算简单、稳定性强
5. 适合日线级别交易

请给出：
- 因子名称
- 计算公式
- Python 实现伪代码
- 理论依据（为什么有效）
- 建议持有期和换手率
- 与已有因子的差异性
```

---

## 📞 联系方式

- **作者**：Yezhou Liu
- **邮箱**：YezhouLiu7@gmail.com
- **数据库**：PostgreSQL @ localhost:5432/quant

---

**最后更新**：2026-02-08
