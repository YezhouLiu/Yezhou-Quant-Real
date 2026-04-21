# Implementation Plan: 数据库 Schema 与读写层

**Branch**: `001-database-schema` | **Date**: 2026-04-21 | **Status**: ✅ Implemented  
**Spec**: [spec.md](spec.md)

---

## Summary

用 PostgreSQL 构建一个支持慢量化系统全生命周期的关系型数据仓库：从 US 股票主数据、日度价格、技术因子，到回测持仓快照和实盘交易记录，共 13 张表。所有表通过外键约束保证数据一致性，读写操作通过独立的 `rw_*.py` 模块封装，遵循 caller-provides-connection 规范。

---

## Technical Context

| 项目 | 值 |
|------|----|
| **Language/Version** | Python 3.10+, SQL |
| **Primary Dependencies** | psycopg2-binary |
| **Storage** | PostgreSQL 14+, localhost:5432, database=quant |
| **Testing** | pytest（tests/database/） |
| **Target Platform** | Linux/Windows 本地服务器 |
| **Project Type** | 数据层 library |
| **Performance Goals** | 批量 10K 行写入 < 10s；快照查询 < 2s |
| **Constraints** | 本地单机部署，不需要连接池 |

---

## Project Structure

```text
database/
├── schema/
│   ├── create_tables.py          # 一键建表入口
│   ├── drop_tables.py            # 一键删表（危险操作，需确认）
│   └── tables/
│       ├── instruments.py        # 资产主数据表 DDL
│       ├── market_prices.py      # 日度价格表 DDL
│       ├── factor_values.py      # 因子值表 DDL
│       ├── fundamental_data.py   # 基本面表 DDL（预留）
│       ├── positions.py          # 实盘持仓表 DDL（预留）
│       ├── fills.py              # 成交记录表 DDL（预留）
│       ├── exp_positions.py      # 回测持仓表 DDL
│       ├── trading_calendar.py   # 交易日历表 DDL
│       ├── corporate_actions.py  # 公司行为表 DDL
│       ├── system_state.py       # 系统状态 KV 表 DDL
│       └── data_update_logs.py   # 更新日志表 DDL
└── readwrite/
    ├── rw_instruments.py         # instruments CRUD
    ├── rw_market_prices.py       # market_prices CRUD
    ├── rw_factor_values.py       # factor_values CRUD（含版本化）
    ├── rw_fundamental_data.py    # fundamental_data CRUD（预留）
    ├── rw_positions.py           # positions CRUD（预留）
    ├── rw_fills.py               # fills CRUD（预留）
    ├── rw_exp_positions.py       # exp_positions + NAV 聚合
    ├── rw_trading_calendar.py    # 交易日历查询
    ├── rw_corporate_actions.py   # 公司行为 CRUD
    ├── rw_system_state.py        # state_key → state_value KV
    └── rw_data_update_logs.py    # 更新日志写入
```

---

## Table Schema Reference

### instruments（资产主数据）

| 列名 | 类型 | 说明 |
|------|------|------|
| instrument_id | SERIAL PK | 主键 |
| ticker | VARCHAR | 交易代码 |
| exchange | VARCHAR | 交易所 |
| asset_type | VARCHAR | Stock / ETF / Cash |
| status | VARCHAR | active / delisted |
| is_tradable | BOOLEAN | 是否在当前宇宙 |
| sector | VARCHAR | GICS 板块（nullable） |
| industry | VARCHAR | 行业（nullable） |
| ipo_date | DATE | 上市日期 |
| delist_date | DATE | 退市日期（nullable） |

唯一约束: `(ticker, exchange)`

---

### market_prices（日度价格）

| 列名 | 类型 | 说明 |
|------|------|------|
| instrument_id | INT FK | 外键 → instruments |
| date | DATE | 交易日 |
| open / high / low / close | NUMERIC | 原始 OHLC |
| volume | BIGINT | 成交量 |
| adj_close | NUMERIC | 前复权收盘价 |
| adj_volume | BIGINT | 前复权成交量 |
| dividends | NUMERIC | 分红（Tiingo 直接提供） |
| stock_splits | NUMERIC | 拆股比例 |

主键: `(instrument_id, date)`；索引: `(date)`, `(instrument_id, date)`

---

### factor_values（因子值）

| 列名 | 类型 | 说明 |
|------|------|------|
| instrument_id | INT FK | 外键 → instruments |
| date | DATE | 计算日期 |
| factor_name | VARCHAR | 因子名（如 `mom_63d_skip21`） |
| factor_version | VARCHAR | 版本标识 |
| factor_value | NUMERIC(38,10) | 因子数值（高精度） |
| factor_args | JSONB | 计算参数（如 `{window: 63}`) |
| config | JSONB | 计算时的全局配置快照 |

主键: `(instrument_id, date, factor_name, factor_version)`  
索引: `(factor_name, date, factor_version)`, `(instrument_id, date)`, `(date)`

---

### exp_positions（回测持仓快照）

| 列名 | 类型 | 说明 |
|------|------|------|
| date | DATE | 快照日期 |
| instrument_id | INT | 证券 ID（0=现金） |
| quantity | NUMERIC | 持仓数量（现金时=金额） |
| buy_price | NUMERIC | 买入均价 |
| current_price | NUMERIC | 当日收盘价 |
| market_value | NUMERIC | 市值 |

主键: `(date, instrument_id)`

---

## Constitution Check

| 原则 | 状态 | 说明 |
|------|------|------|
| I. 数据优先 | ✅ | 所有外部数据必须落地 DB 再使用 |
| II. 增量更新 | ✅ | system_state 表追踪断点 |
| III. 版本化因子 | ✅ | (factor_name, factor_version) 复合键 |
| IV. 配置集中化 | ✅ | DB 连接参数来自 config.yaml |
| VIII. 简洁优先 | ✅ | 纯 psycopg2，无 ORM |

---

## Key Design Decisions

1. **caller-provides-connection**: 所有 rw_* 函数接受 `conn` 参数，便于事务管理
2. **ON CONFLICT DO UPDATE**: instruments 和 market_prices 均使用 upsert，支持幂等写入
3. **instrument_id=0 为现金**: 硬编码常量 `CASH_INSTRUMENT_ID=0`，不在 instruments 表建记录
4. **factor_value NUMERIC(38,10)**: 保留 10 位小数，避免浮点精度损失
5. **JSONB 存储 factor_args**: 支持未来因子参数结构变化时不需要 DDL 变更
