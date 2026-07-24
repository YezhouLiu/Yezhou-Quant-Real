# Implementation Plan: 每日市场情报简报

**Branch**: `007-daily-briefing` | **Date**: 2026-07-24 | **Status**: ✅ Implemented  
**Spec**: [spec.md](spec.md)

---

## Summary

在已有的 9 个技术因子基础上，新增 3 个面向日内情报的因子（单日收益率、量比、连续下跌天数），
并构建 `reports/daily_briefing.py` 模块，每日生成结构化的分类榜单报告，供人工研究使用。

---

## Technical Context

| 项目 | 值 |
|------|----|
| **Language/Version** | Python 3.10+, pandas 2.x |
| **Primary Dependencies** | pandas, psycopg2（直接 SQL，无 ORM） |
| **Storage** | 读取 factor_values + market_prices；简报不写库 |
| **Testing** | pytest（tests/factors/ + tests/reports/）|
| **新增文件数** | 10 个（新建）+ 3 个（修改）|

---

## Project Structure（变更后）

```text
factors/
├── momentum.py              # ⭐ 扩展：MOMENTUM_SPECS 新增 ("mom_1d", 1, 0)
├── volume_ratio.py          # ✅ 新增：vol_ratio_20d 因子
├── decline_streak.py        # ✅ 新增：decline_streak 因子
└── ...（原有 5 个不变）

engine/compute_factors/
├── compute_all_factors.py   # ⭐ 扩展：新增 compute_volume_ratio / compute_decline_streak
├── compute_volume_ratio.py  # ✅ 新增：批量计算 runner
├── compute_decline_streak.py# ✅ 新增：批量计算 runner
└── ...（原有 6 个不变）

reports/                     # ✅ 新增目录
├── __init__.py
└── daily_briefing.py        # ✅ 核心简报模块

tasks/
└── daily_tasks.py           # ⭐ 扩展：daily_update() 末尾调用 run_briefing()

tests/
├── factors/
│   ├── test_volume_ratio.py # ✅ 新增：6 个测试用例
│   └── test_decline_streak.py # ✅ 新增：7 个测试用例
└── reports/
    ├── __init__.py           # ✅ 新增
    └── test_daily_briefing.py # ✅ 新增：10 个测试用例
```

---

## New Factor Algorithms

### `vol_ratio_20d`（量比因子）

```
vol_ratio_20d[t] = adj_volume[t] / mean(adj_volume[t-20:t-1])
```

- 分母使用 `.shift(1)` 确保只包含过去 20 天（不含今天），检测今日相对异动
- `adj_volume = 0` 的行不纳入均值计算（mask 处理）
- `vol_ratio = 0` 或 NaN 的行不写入 `factor_values`
- 预热 buffer = `(window + 10) * 2` 天

**Factor name**: `vol_ratio_{window}d`，当前只计算 `vol_ratio_20d`

### `decline_streak`（连续下跌计数）

```
is_down[t] = (adj_close[t] < adj_close[t-1])     # 严格小于，持平不算
streak[t]  = cumulative count of consecutive True in is_down
```

向量化实现（避免 Python 循环）：
```python
flip_groups = (~df["is_down"]).cumsum()
df["decline_streak"] = df["is_down"].groupby(flip_groups).cumsum().astype(int)
```

- 在**完整** `df`（含 buffer）上计算 `is_down` 和 `streak`，再截取目标日期范围
- target 切片的第一行不会因为 `.shift()` 而错误地被排除
- 预热 buffer = `_MAX_STREAK_CAP * 2 = 504` 天（捕捉超长跌势）

**Factor name**: `decline_streak`（无参数变体，唯一）

### `mom_1d`（单日收益率）

直接扩展已有 `factors/momentum.py`，在 `MOMENTUM_SPECS` 中新增：
```python
("mom_1d", 1, 0)   # lookback=1, skip=0  → factor_name = "mom_1d"
```
无需新文件，利用已有框架。

---

## Data Flow

```
market_prices
    ↓
factors/volume_ratio.py            → factor_values (vol_ratio_20d)
factors/decline_streak.py          → factor_values (decline_streak)
factors/momentum.py (mom_1d added) → factor_values (mom_1d)
    ↓
reports/daily_briefing.py
    ├─ _get_latest_factor_date()   → "SELECT MAX(date) FROM factor_values"
    ├─ _load_factor_snapshot()     → JOIN factor_values × instruments（wide pivot）
    ├─ _load_price_snapshot()      → JOIN market_prices × instruments（当日 OHLCV）
    │
    ├─ momentum_leaders   (nlargest by mom_5d)
    ├─ momentum_laggards  (nsmallest by mom_5d)
    ├─ volume_spikes      (vol_ratio_20d >= 2.0)
    ├─ decline_streaks    (decline_streak >= 3)
    ├─ volatility_alerts  (vol_20d_ann252 >= 0.50)
    └─ sector_summary     (groupby sector, agg mean mom_5d)
         ↓
    format_briefing() → 格式化字符串 → log.info()
```

---

## Briefing Module API

```python
# 生成结构化数据
data = generate_briefing(conn=conn, date="2026-07-24", top_n=20)
# data = {
#   "date": "2026-07-24",
#   "sections": [
#     {"name": "momentum_leaders", "label": "...", "rows": [...]},
#     ...
#   ],
#   "raw_df": pd.DataFrame  # 宽表，供上层自定义分析
# }

# 格式化为字符串
text = format_briefing(data)

# 一站式：生成 + 日志输出 + 返回字符串
text = run_briefing(date=None, top_n=20)   # date=None → 使用最新日期
```

---

## Factor Naming Convention（完整更新）

| 因子 | 命名模板 | 示例 | 新增 |
|------|---------|------|------|
| 动量 | `mom_{lookback}d` / `mom_{lookback}d_skip{skip}` | `mom_5d`, `mom_1d`, `mom_252d_skip21` | `mom_1d` ✅ |
| 波动率 | `vol_{window}d_ann{annualize}` | `vol_20d_ann252` | — |
| 美元成交量 | `dv_{window}d_log` | `dv_20d_log` | — |
| 最大回撤 | `mdd_{window}d` | `mdd_252d` | — |
| 波动率之波动 | `volvol_{volvol_window}d_from_vol{vol_window}d` | — | — |
| 跳跃风险 | `jump_risk_{window}d` | — | — |
| **量比** | `vol_ratio_{window}d` | `vol_ratio_20d` | ✅ 新增 |
| **连续下跌** | `decline_streak` | `decline_streak` | ✅ 新增 |

---

## Briefing Sections Reference

| Section name | 数据来源 | 排序 | 阈值 |
|---|---|---|---|
| `momentum_leaders` | `mom_5d` | 降序 | 无（取 top_n） |
| `momentum_laggards` | `mom_5d` | 升序 | 无（取 bottom_n） |
| `volume_spikes` | `vol_ratio_20d` | 降序 | `>= 2.0x` |
| `decline_streaks` | `decline_streak` | 降序 | `>= 3 天` |
| `volatility_alerts` | `vol_20d_ann252` | 降序 | `>= 50%` |
| `sector_summary` | `mom_5d`（聚合） | avg_mom_5d 降序 | 无 |

---

## Tests Coverage

| 文件 | 用例数 | 覆盖场景 |
|------|--------|---------|
| `test_volume_ratio.py` | 6 | 量突变检测、均值为 1、buffer 验证、零量、空数据、日期倒序 |
| `test_decline_streak.py` | 7 | 连跌计数、重置、持平、全涨、buffer、空数据、日期倒序 |
| `test_daily_briefing.py` | 10 | section 完整性、top_n 限制、空数据、无日期、量阈值过滤、连跌阈值过滤、字符串格式、关键词存在、板块汇总 |

**总计**：23 个测试用例，全部通过 ✅

---

## Constitution Check

| 原则 | 状态 | 说明 |
|------|------|------|
| I. 数据优先 | ✅ | 简报从 DB 查询，不绕过数据库 |
| II. 增量更新 | ✅ | 新因子遵循 system_state + buffer 模式 |
| III. 版本化因子 | ✅ | 两个新因子均携带 factor_version='v1' 和 factor_args |
| V. 模块化信号管道 | ✅ | generate_briefing / format_briefing 职责分离 |
| VIII. 简洁优先 | ✅ | 简报不新增数据库表，直接查询已有表 |

---

## Future Extensions

| 功能 | 描述 | 状态 |
|------|------|------|
| Tiingo News 情绪 | 拉取 `GET /tiingo/news?tickers=X` 并做标题情绪打分 → 加入 NEWS SENTIMENT section | 🔴 未实现 |
| 52 周高低位 | `dist_from_52w_high` 因子 → "距高点跌多深" section | 🔴 未实现 |
| 日内波动率 | `intraday_range_pct = (high-low)/open` 存入因子表 | 🔴 未实现 |
| 基本面价值因子 | P/B、P/E 来自 SEC EDGAR fundamentals | 🔴 依赖 spec-003 基本面接入 |
| 简报 HTML 输出 | 将文字报告导出为 HTML 文件，支持在浏览器查看 | 🔴 未实现 |
