# Yezhou-Quant-Real Constitution

> 慢量化 · 价值投资系统的架构宪法

## Core Principles

### I. 数据优先（Data-First）

一切计算、回测、策略均依赖数据库中已落地的数据。  
绝不从内存/文件直接计算后绕开数据库写出结果。  
数据流向：外部 API → 批量写入 PostgreSQL → 从 DB 读取用于计算。

### II. 增量更新（Incremental Updates）

所有下载器均从 `system_state` 表读取上次更新日期，只拉取增量数据。  
禁止在脚本层硬编码起始日期；起始日期由 `config.yaml` 统一管制。

### III. 版本化因子（Versioned Factors）

所有因子写入 `factor_values` 时必须携带 `factor_version` 和 `factor_args`（JSONB）。  
不同参数组合（如 window=20 vs window=60）视为不同版本，可共存于同一表中。  
同一（instrument_id, date, factor_name, factor_version）组合为唯一键，避免重复写入。

### IV. 配置集中化（Config Centralization）

所有可调参数（price_floor, slippage, window 等）必须定义在 `config/config.yaml`。  
代码中禁止出现业务意义的魔法数字；通过 `config_loader.py` 统一读取。  
敏感信息（API 密钥、DB 密码）存放于 `secrets.env`，不纳入版本控制。

### V. 模块化信号管道（Modular Signal Pipeline）

信号计算管道分四层，每层只关注自己的职责：  
`DB → 长表 → 宽表 → 归一化 → 评分 → 选股 → 组合`  
每层的输入/输出均为 pandas DataFrame，接口清晰，易于替换。

### VI. 回测与实盘分离（Backtest / Live Separation）

回测结果写入 `exp_positions` 表，实盘持仓写入 `positions` 表。  
两张表结构相似但互不干扰，实盘追踪仍处于预留状态（研究模式）。

### VII. 成本真实性（Realistic Cost Model）

回测中的组合再平衡必须计入：  
- 滑点（`slippage = 0.5%`）  
- 交易佣金（`transaction_cost = 0.1%`）  
- 交易所费用（`exchange_cost = 0.05%`）  
- 再投资比例（`reinvest_ratio = 98%`，保留 2% 现金缓冲）

### VIII. 简洁优先（Simplicity First）

- 每个 Python 模块只做一件事
- 优先使用 psycopg2 原生 SQL，不引入 ORM 层
- 不为"将来可能用到"的功能预先抽象

## Brownfield 约束

- 本项目为**已有运行代码的棕地项目**，spec 文档描述的是已实现的功能现状
- 新功能按 spec-kit 流程先写 spec → plan → tasks，再实施
- 对已有代码的修改需同步更新对应 spec

## Data Architecture

```
外部数据源
  ├── Tiingo EOD API  →  market_prices（价格/股息/拆股）
  ├── SEC EDGAR XBRL  →  fundamental_data（基本面指标）
  ├── Wikipedia/iShares →  csv/（可交易候选宇宙）
  └── yfinance         →  instruments.sector/industry

PostgreSQL（本地 localhost:5432）
  ├── 核心数据表：instruments, market_prices, trading_calendar
  ├── 因子表：factor_values
  ├── 基本面表：fundamental_data（预留）
  ├── 回测表：exp_positions
  ├── 实盘表：positions, fills（预留）
  └── 辅助表：system_state, data_update_logs, corporate_actions
```

## Technology Stack

| 层次 | 技术 |
|------|------|
| 数据库 | PostgreSQL 14+, psycopg2 |
| 数据处理 | Python 3.10+, pandas, numpy |
| HTTP 请求 | requests + 指数退避重试 |
| 可视化 | matplotlib |
| 配置 | PyYAML, python-dotenv |
| 测试 | pytest |

## Governance

本宪法优先于所有其他惯例。  
违反核心原则的改动需在 PR 说明中记录原因并获 owner 批准。  
每次重大架构变更需更新本文件的 Last Amended 日期。

**Version**: 1.0.0 | **Ratified**: 2026-04-21 | **Last Amended**: 2026-04-21
