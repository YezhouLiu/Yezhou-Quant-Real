# Implementation Plan: 因子计算引擎

**Branch**: `003-factor-computation` | **Date**: 2026-04-21 | **Status**: ✅ Implemented  
**Spec**: [spec.md](spec.md)

---

## Summary

从 PostgreSQL 的 `market_prices` 表加载前复权价格序列，使用 pandas rolling 运算计算 6 类技术因子（动量/波动率/成交量/回撤/波动率之波动率/跳跃风险），将结果批量写回 `factor_values` 表，同时提供跨截面归一化管道（rank + magnitude）供信号合成使用。

---

## Technical Context

| 项目 | 值 |
|------|----|
| **Language/Version** | Python 3.10+, pandas 2.x, numpy |
| **Primary Dependencies** | pandas, numpy, psycopg2 |
| **Storage** | PostgreSQL（factor_values 表） |
| **Testing** | pytest（tests/factors/）|
| **Target Platform** | Windows/Linux 本地 |
| **Project Type** | 计算 library + orchestrator CLI |
| **Performance Goals** | 1K 只股票 × 252 天/因子 < 5 分钟 |
| **Constraints** | 内存限制：避免加载全量历史到内存，按年份分批 |

---

## Project Structure

```text
factors/
├── volatility.py              # 年化波动率因子
├── momentum.py                # 价格动量因子
├── dollar_volume.py           # 对数美元成交量因子
├── max_drawdown.py            # 最大回撤因子
├── volatility_of_volatility.py # 波动率之波动率
└── jump_risk.py               # 跳跃风险

engine/
├── signals.py                 # 因子加载 → 宽表 → 归一化管道
├── normalizer.py              # robust_zscore() 工具函数
└── compute_factors/
    ├── compute_all_factors.py # 调度所有因子的入口
    ├── compute_momentum.py    # momentum 独立 runner
    ├── compute_volatility.py  # volatility 独立 runner
    ├── compute_dollar_volume.py
    ├── compute_max_drawdown.py
    ├── compute_vol_of_vol.py
    └── compute_jump_risk.py
```

---

## Factor Computation Pattern（统一算法模板）

所有 6 个因子均遵循以下算法框架：

```python
def compute_factor(conn, start_date, end_date, params):
    # 1. 计算数据加载范围（含预热窗口）
    load_start = start_date - timedelta(days=buffer_days)
    
    # 2. 从 DB 加载 adj_close 序列（长表）
    prices = get_prices(conn, load_start, end_date, asset_types=['Stock', 'ETF'])
    
    # 3. 转宽表（pivot） → 运行 pandas rolling 计算
    wide = prices.pivot(index='date', columns='instrument_id', values='adj_close')
    factor_wide = wide.rolling(window=params.window).apply(formula)
    
    # 4. 截断至目标日期范围，去除预热期
    factor_wide = factor_wide[factor_wide.index >= start_date]
    
    # 5. 转长表（melt），过滤 NaN
    rows = factor_wide.melt(...).dropna()
    
    # 6. 批量写入 factor_values（含 version + args）
    batch_insert_factor_values(conn, rows, factor_name=..., factor_version=..., factor_args=...)
```

---

## Signal Pipeline（归一化管道）

```
DB.factor_values
    └─ get_factor_values(date, factor_name)    → 长表 DataFrame
        └─ pivot → 宽表（instrument_id × factor_name）
            └─ normalize_cross_section(df, specs)
                ├─ [method='rank']  → rank_normalize()      → [-1, 1] 排名信号
                └─ [method='mag']   → magnitude_normalize() → [-1, 1] robust z-score + activation
                    └─ robust_zscore(values) = (x - median) / MAD
```

### `rank_normalize()`

```
1. 对每列做百分位排名（0~1）
2. 线性映射到 [-1, 1]
3. 若 ascending=False，翻转符号
```

### `magnitude_normalize()`

```
1. robust_zscore: (x - median) / MAD（抗离群值）
2. clip: 超过 z_clip=6.0 的值截断
3. 激活函数：tanh（默认）或 sigmoid → 压缩到 (-1, 1)
4. 若 ascending=False，×(-1)
```

### `build_signals_for_date()`

```python
def build_signals_for_date(conn, asof_date, specs, factor_version, universe_ids):
    # 1. 遍历 specs 中的每个 factor_name
    # 2. get_factor_values(conn, asof_date, factor_name, factor_version)
    # 3. pivot → wide 宽表
    # 4. normalize_cross_section(wide, specs)
    # 5. 过滤至 universe_ids
    # 返回：归一化后的宽表 DataFrame
```

---

## Factor Naming Convention

| 因子 | 模板 | 示例 |
|------|------|------|
| 动量 | `mom_{lookback}d_skip{skip}` | `mom_63d_skip21` |
| 波动率 | `vol_{window}d_ann{annualize}` | `vol_60d_ann252` |
| 美元成交量 | `dv_{window}d_log` | `dv_20d_log` |
| 最大回撤 | `mdd_{window}d` | `mdd_252d` |
| 波动率之波动 | `vol_of_vol_{window}d` | `vol_of_vol_60d` |
| 跳跃风险 | `jump_risk_{window}d` | `jump_risk_252d` |

---

## Constitution Check

| 原则 | 状态 | 说明 |
|------|------|------|
| I. 数据优先 | ✅ | 从 DB 加载价格，结果写回 DB |
| III. 版本化因子 | ✅ | factor_version + factor_args JSONB |
| IV. 配置集中化 | ✅ | window/annualize 等参数来自 config.yaml |
| VIII. 简洁优先 | ✅ | 每个因子文件独立，统一算法框架 |

---

## Future Extensions（方向 A：基本面因子）

| 因子 | 所需数据 | 状态 |
|------|---------|------|
| E/P（Earnings Yield） | EPS from fundamental_data | 🔴 未实现 |
| FCF/P（自由现金流收益率） | FCF from fundamental_data | 🔴 未实现 |
| ROE/ROIC | NI, Equity from fundamental_data | 🔴 未实现 |
| Piotroski F-Score | 9 个会计维度 | 🔴 未实现 |
| P/B 倒数 | BVPS from fundamental_data | 🔴 未实现 |
