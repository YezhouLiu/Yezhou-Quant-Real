# Yezhou-Quant-Real · Spec Kit

> Spec-Driven Development 文档集 —— 慢量化价值投资系统

本目录按照 [github/spec-kit](https://github.com/github/spec-kit) 规范组织，是对已有代码库（brownfield）的逆向规格文档化。

---

## 项目架构速览

```
慢量化系统端到端数据流：

外部数据源（Tiingo / SEC EDGAR / Wikipedia）
    │
    ▼
data_download/（下载管道）
    │   price_downloader.py  ←→  Tiingo EOD API
    │   fundamentals_downloader.py ←→ SEC EDGAR XBRL
    │   tradable_candidates.py ←→ Russell/S&P/iShares
    │
    ▼
PostgreSQL 数据库（13 张表）
    │   instruments · market_prices · factor_values
    │   fundamental_data · exp_positions · trading_calendar
    │   positions · fills · corporate_actions · system_state ···
    │
    ├─ factors/（因子计算：6 个技术因子）
    │       momentum · volatility · dollar_volume
    │       max_drawdown · vol_of_vol · jump_risk
    │           │
    │           ▼
    │       factor_values 表（versioned）
    │
    └─ engine/（回测引擎）
            signals.py → normalize_cross_section()
            ScoringStrategy → LinearScorer → TopKSelector
            BacktestRunner → Portfolio.rebalance()
                │
                ▼
            exp_positions 表（NAV 时间序列）
                │
                ▼
            ui/（可视化：NAV vs SPY/QQQ）
```

---

## Spec 文档索引

| # | Feature | 状态 | 核心文件 |
|---|---------|------|---------|
| 001 | [数据库 Schema 与读写层](specs/001-database-schema/) | ✅ 已实现 | `database/schema/` `database/readwrite/` |
| 002 | [价格数据下载管道](specs/002-price-data-pipeline/) | ✅ Tiingo 已实现 / ⚠️ SEC EDGAR 待打通 | `data_download/input/` |
| 003 | [因子计算引擎](specs/003-factor-computation/) | ✅ 6 个技术因子已实现 | `factors/` `engine/compute_factors/` |
| 004 | [回测引擎](specs/004-backtest-engine/) | ✅ 月度再平衡已实现 | `engine/` |
| 005 | [可交易宇宙管理](specs/005-trading-universe/) | ✅ 已实现 | `data_download/update/` |
| 006 | [任务调度与可视化](specs/006-task-orchestration/) | ✅ 手动触发 / ⚠️ 自动调度未实现 | `tasks/` `ui/` |

---

## 使用约定

### 已实现功能的 Spec
每个 spec 目录包含：
- `spec.md` — 功能规格（用户故事 + 验收场景 + 需求）
- `plan.md` — 实现计划（技术选型 + 架构 + 关键决策）

### 新功能的开发流程（SDD）
1. 在 `specs/` 下创建新编号目录（如 `007-fundamental-factors/`）
2. 运行 `/speckit.specify` 或手动填写 `spec.md`（用户故事 + 验收场景）
3. 运行 `/speckit.plan` 或手动填写 `plan.md`（技术设计）
4. 运行 `/speckit.tasks` 生成 `tasks.md`（执行任务列表）
5. 实施代码，完成后更新 spec 中的 Status

---

## 待实现的高优先级 Spec（来自 TODO.md）

| 优先级 | 方向 | 预计 Spec 编号 |
|--------|------|---------------|
| 🔴 P0 | 基本面估值因子（E/P, FCF/P）—— 方向 A1 | 007 |
| 🟠 P1 | 回测分析体系（Sharpe/IC/分位数）—— 方向 B | 008 |
| 🟠 P1 | 基本面日度化 + 股息数据 —— 方向 E1/E2 | 009 |
| 🟡 P2 | Piotroski F-Score 质量因子 —— 方向 A2 | 010 |
| 🟡 P2 | 股息成长策略 —— 方向 D1 | 011 |
| 🟢 P3 | 自动调度（APScheduler）—— 方向 F | 012 |
| 🟢 P3 | Web Dashboard（Streamlit）—— 方向 G | 013 |

---

## 宪法

> 见 [constitution.md](constitution.md) —— 项目架构原则与技术栈约定

核心原则摘要：
1. **数据优先** — 所有数据必须先落地 PostgreSQL，再用于计算
2. **增量更新** — 通过 `system_state` 追踪断点，不重复下载
3. **版本化因子** — `(factor_name, factor_version)` 复合键，多版本共存
4. **配置集中化** — 所有参数来自 `config.yaml`，secrets 来自 `secrets.env`
5. **模块化信号管道** — 严格分层：信号→评分→选股→组合

---

*本 spec 文档集于 2026-04-21 生成；由 GitHub Copilot 辅助写作，项目负责人审阅。*
