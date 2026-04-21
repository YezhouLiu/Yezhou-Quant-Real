# Implementation Plan: 价格数据下载管道

**Branch**: `002-price-data-pipeline` | **Date**: 2026-04-21 | **Status**: ✅ Implemented  
**Spec**: [spec.md](spec.md)

---

## Summary

通过 Tiingo EOD API 实现日度价格的增量下载，以 `system_state` 表追踪断点；通过 SEC EDGAR XBRL API 实现季度基本面数据下载；通过 Wikipedia/iShares 解析维护可交易宇宙候选列表。所有下载器均包含重试逻辑、失败率控制和操作日志。

---

## Technical Context

| 项目 | 值 |
|------|----|
| **Language/Version** | Python 3.10+ |
| **Primary Dependencies** | requests, pandas, yfinance, python-dotenv |
| **Storage** | PostgreSQL（via psycopg2） + CSV 文件（csv/） |
| **Testing** | pytest（tests/data_download/） |
| **Target Platform** | Windows/Linux 本地机器 |
| **Project Type** | CLI 脚本 + library 模块 |
| **Performance Goals** | 增量下载 1000 只股票/日 < 10 分钟 |
| **Constraints** | Tiingo 速率限制（varies by tier）；SEC EDGAR ≤ 10 req/s |

---

## Project Structure

```text
data_download/
├── input/
│   ├── price_downloader.py           # Tiingo EOD 批量下载（主力模块）
│   ├── fundamentals_downloader.py    # SEC EDGAR XBRL 下载
│   ├── all_us_stocks.py              # Tiingo supported_tickers 全量列表
│   ├── tradable_candidates.py        # 可交易候选宇宙（Russell/S&P/手动）
│   ├── build_trading_calendar.py     # 交易日历构建
│   └── corporate_actions_extractor.py # 公司行为提取（从 market_prices 派生）
├── update/
│   ├── update_tradable_universe.py   # 每日同步 is_tradable 标志
│   ├── update_universe.py            # 宇宙维护
│   └── fill_sector_industry_yfinance.py # 行业/板块元数据补充
└── *.bat                             # Windows 执行脚本
```

---

## Core Download Algorithm: `price_downloader.py`

```
1. 读取 system_state.last_price_date → 确定 start_date
2. 获取 instruments 表中 is_tradable=True 的 ticker 列表
3. 按 batch_size=500 分批，每批并行/串行请求 Tiingo API
4. 对每只 ticker：
   a. GET https://api.tiingo.com/tiingo/daily/{ticker}/prices?startDate=...
   b. 返回 JSON → 转 DataFrame → ON CONFLICT DO UPDATE 写入 market_prices
   c. 失败 → 计入失败计数，超 max_failure_rate 终止
5. 成功后更新 system_state.last_price_date = today
6. 写入 data_update_logs（source='tiingo_prices', status, record_count）
```

### 重试策略

| 场景 | 处理 |
|------|------|
| HTTP 429 | 指数退避（1s→2s→4s→8s），最多 5 次 |
| HTTP 5xx | 退避重试，最多 3 次 |
| HTTP 404 | 记录 warning，跳过该 ticker |
| 连接超时 | 设置 timeout=30s，超时后重试 |

---

## Tiingo API 数据映射

| Tiingo 字段 | DB 列名 | 说明 |
|------------|---------|------|
| `date` | `date` | 交易日（YYYY-MM-DD） |
| `open` | `open` | 原始开盘价 |
| `high` | `high` | 原始最高价 |
| `low` | `low` | 原始最低价 |
| `close` | `close` | 原始收盘价 |
| `volume` | `volume` | 原始成交量 |
| `adjClose` | `adj_close` | 前复权收盘价（用于因子计算）|
| `adjVolume` | `adj_volume` | 前复权成交量 |
| `divCash` | `dividends` | 现金分红金额 |
| `splitFactor` | `stock_splits` | 拆股比例 |

---

## SEC EDGAR 下载策略

```
API 端点: https://data.sec.gov/api/xbrl/companyfacts/{CIK}.json

映射逻辑:
  XBRL Concept → metric_name
  value        → metric_value
  filed        → report_date
  start/end    → period_start/period_end
  form (10-Q/10-K) → period_type

速率控制: time.sleep(0.25) 在每次请求后

CIK 获取: 通过 SEC EDGAR company search API，用 ticker 查 CIK
```

---

## 可交易宇宙来源优先级

| 优先级 | 来源 | 备注 |
|--------|------|------|
| 1（必选）| Russell 1000 - Wikipedia | 必须包含 |
| 2 | Russell 2000 - Wikipedia | 扩大小盘覆盖 |
| 3 | S&P 500 / S&P 400 / S&P 600 | 补充流动性良好标的 |
| 4 | iShares ETF 成分 | 覆盖 ETF 级别 |
| 5 | 手动添加（preserved） | 用于特定研究目的 |

---

## Constitution Check

| 原则 | 状态 | 说明 |
|------|------|------|
| I. 数据优先 | ✅ | 所有数据落地 DB 再用 |
| II. 增量更新 | ✅ | system_state 记录断点 |
| IV. 配置集中化 | ✅ | API key 从 secrets.env，参数从 config.yaml |
| VIII. 简洁优先 | ✅ | 每个下载器独立，无依赖耦合 |

---

## Known Issues & Future Work

| 问题 | 状态 | 计划 |
|------|------|------|
| 基本面 rw 列名不匹配（as_of_date vs report_date） | 🔴 搁置 | 等基本面因子需求启动时修复 |
| SEC EDGAR 链路未完整测试 | ⚠️ 待验证 | 等因子 A1 开发时端到端测试 |
| fundamental_daily 表无下载器 | ⚠️ 缺失 | E1 方向实现 |
