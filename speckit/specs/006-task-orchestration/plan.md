# Implementation Plan: 任务调度与可视化

**Branch**: `006-task-orchestration` | **Date**: 2026-04-21 | **Status**: ✅ Partially Implemented  
**Spec**: [spec.md](spec.md)

---

## Summary

提供三类任务入口：①每日数据更新流水线（download→factor→universe）②回测执行任务（定义策略并运行 BacktestRunner）③ NAV 可视化（回测组合 vs SPY 等基准的归一化收益对比）。当前所有任务均为手动触发，自动调度为待实现扩展。

---

## Technical Context

| 项目 | 值 |
|------|----|
| **Language/Version** | Python 3.10+ |
| **Primary Dependencies** | matplotlib, pandas, psycopg2 |
| **Storage** | 读：market_prices + factor_values + exp_positions；写：exp_positions |
| **Testing** | pytest（tasks/ 目前无自动化测试） |
| **Target Platform** | Windows（.bat 脚本触发），Linux（shell 脚本触发）|
| **Project Type** | Task runner + visualization |
| **Performance Goals** | daily_update < 30 分钟；NAV 图 < 5 秒 |
| **Constraints** | 手动触发；无自动调度 |

---

## Project Structure

```text
tasks/
├── daily_tasks.py        # daily_update()：每日数据更新流水线
├── backtest_tasks.py     # run_backtest()：策略定义 + 回测执行
├── annual_tasks.py       # annual_update()：年度任务（宇宙刷新等）
├── seasonal_tasks.py     # seasonal_update()：季度任务（SEC EDGAR 下载等）
└── logs/                 # 任务执行日志（rotating file）

ui/
├── api.py                          # compare_portfolio_with_tickers()：可视化入口
├── data_sources/
│   ├── portfolio_nav.py            # PortfolioNAVSource：从 exp_positions 加载 NAV
│   └── ticker_nav.py              # TickerNAVSource：从 market_prices 加载 ticker 收益
└── plots/
    └── nav_plot.py                 # plot_nav()：matplotlib 绘图

main.py                             # 顶层入口（演示用）
```

---

## Daily Update Pipeline

```python
def daily_update(conn):
    """每日在收盘后手动运行"""
    today = DATE_TODAY()
    
    # Step 1: 价格增量下载
    try:
        download_prices(conn)
        log_update(conn, source='tiingo_prices', status='success')
    except Exception as e:
        log_update(conn, source='tiingo_prices', status='failed', error=str(e))
        raise  # 价格失败则不继续
    
    # Step 2: 计算全部因子（基于今日价格）
    compute_all_factors(conn, date=today)
    log_update(conn, source='factor_compute', status='success')
    
    # Step 3: 更新可交易宇宙
    update_tradable_universe(conn)
    log_update(conn, source='universe_update', status='success')
```

---

## Backtest Task Configuration（`backtest_tasks.py`）

```python
def run_backtest(conn, overwrite: bool = False):
    # ① 定义因子规格
    factor_specs = [
        FactorSpec('mom_63d_skip21', ascending=True,  methods=['rank']),
        FactorSpec('vol_60d_ann252', ascending=False, methods=['rank']),
        FactorSpec('mdd_252d',       ascending=False, methods=['rank']),
    ]
    
    # ② 定义评分器（加权线性合成）
    scorer = LinearScorer(weights={
        'mom_63d_skip21': 0.50,
        'vol_60d_ann252': 0.30,
        'mdd_252d':       0.20,
    })
    
    # ③ 定义选股器（Top-5，按综合分排序）
    selector = TopKSelector(k=5, sort_by='score', sort_ascending=False)
    
    # ④ 定义策略
    strategy = ScoringStrategy(factor_specs=factor_specs, scorer=scorer)
    
    # ⑤ 运行回测
    runner = BacktestRunner(
        strategy=strategy,
        selector=selector,
        rebalance_day='last',
        initial_capital=100_000,
    )
    runner.run(conn, start_date='2019-01-01', end_date='2026-04-21', overwrite=overwrite)
```

---

## Visualization Pipeline

```python
def compare_portfolio_with_tickers(conn, benchmark_tickers=['SPY']):
    # 加载策略 NAV
    portfolio_nav = PortfolioNAVSource(conn).load()  # exp_positions → SUM(market_value)
    
    # 加载基准 NAV
    benchmark_navs = {
        ticker: TickerNAVSource(conn, ticker).load()
        for ticker in benchmark_tickers
    }
    
    # 对齐日期范围，归一化至起始 = 100
    all_navs = {'Portfolio': portfolio_nav, **benchmark_navs}
    normalized = align_and_normalize(all_navs)
    
    # 绘图
    plot_nav(normalized)
```

### NAV 归一化方法

```python
# normalized[date] = (nav[date] / nav[start_date]) × 100
start = series.iloc[0]
normalized = (series / start) * 100
```

---

## Execution Scripts（Windows .bat）

| 脚本 | 调用 |
|------|------|
| `data_download/download_prices.bat` | `python -m data_download.input.price_downloader` |
| `data_download/generate_candidates.bat` | `python -m data_download.input.tradable_candidates` |
| `data_download/update_universe.bat` | `python -m data_download.update.update_tradable_universe` |
| `data_download/fill_sector_industry.bat` | `python -m data_download.update.fill_sector_industry_yfinance` |

---

## Future: Automated Scheduling（方向 F）

当前状态：所有任务手动触发
计划方案：Windows Task Scheduler 或 APScheduler

```
每个交易日 20:00（东部时间，即北京时间 09:00 次日）:
    python tasks/daily_tasks.py

每季度末最后一个交易日:
    python tasks/seasonal_tasks.py

每年 12 月 31 日:
    python tasks/annual_tasks.py
```

---

## Constitution Check

| 原则 | 状态 | 说明 |
|------|------|------|
| I. 数据优先 | ✅ | 所有数据先落地 DB |
| IV. 配置集中化 | ✅ | 参数从 config.yaml 读取 |
| VI. 回测/实盘分离 | ✅ | exp_positions 独立于 positions |
| VIII. 简洁优先 | ✅ | 每个任务文件单一职责 |
