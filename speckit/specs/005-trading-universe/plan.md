# Implementation Plan: 可交易宇宙管理

**Branch**: `005-trading-universe` | **Date**: 2026-04-21 | **Status**: ✅ Implemented  
**Spec**: [spec.md](spec.md)

---

## Summary

维护一个动态可交易股票宇宙：通过 tradable_candidates.csv（Russell/S&P 成分）确定候选范围，每日根据最新价格和成交量数据过滤出满足流动性要求的股票，批量更新 `instruments.is_tradable` 标志，确保因子计算和回测只使用高质量的流动性标的。

---

## Technical Context

| 项目 | 值 |
|------|----|
| **Language/Version** | Python 3.10+, pandas |
| **Primary Dependencies** | pandas, requests, psycopg2, BeautifulSoup（Wikipedia 解析）|
| **Storage** | 读：market_prices；写：instruments.is_tradable；CSV |
| **Testing** | pytest（tests/data_download/） |
| **Target Platform** | Windows/Linux 本地 |
| **Project Type** | 日常维护脚本 |
| **Performance Goals** | 5000 只 instruments 的宇宙更新 < 30 秒 |
| **Constraints** | price_floor 必须来自 config.yaml |

---

## Project Structure

```text
data_download/
├── input/
│   ├── tradable_candidates.py          # 多源合并 → tradable_candidates.csv
│   └── all_us_stocks.py                # Tiingo 全量列表 → all_us_stocks_listed.csv
├── update/
│   └── update_tradable_universe.py     # 每日更新 instruments.is_tradable
└── *.bat                               # Windows 执行脚本

csv/
├── tradable_candidates.csv             # 候选宇宙（持久化，preserve_manual）
└── all_us_stocks_listed.csv            # Tiingo 全量参考底库
```

---

## Universe Filtering Logic

```
update_tradable_universe(conn, config):
    1. 加载 tradable_candidates.csv → candidate_tickers (set)
    2. 从 market_prices 获取最近 lookback_days=20 天数据
    3. 计算每只 instrument 的：
       - avg_price    = mean(adj_close, last 20 days)
       - avg_volume   = mean(adj_volume × adj_close, last 20 days)  # 美元成交量
    4. 过滤逻辑：
       is_tradable = (
           ticker in candidate_tickers AND
           avg_price >= config.price.floor AND      # 默认 $5.0
           avg_price <= config.price.ceiling AND    # 默认 $10,000
           avg_volume >= config.price.volume_floor  # 最低美元成交量（如有）
       )
    5. 批量 UPDATE instruments SET is_tradable = ... WHERE ticker IN (...)
```

---

## Tradable Candidates Sources

```
tradable_candidates.py:
    sources = [
        ("russell_1000", wikipedia_url_r1000, required=True),
        ("russell_2000", wikipedia_url_r2000),
        ("sp500",        wikipedia_url_sp500),
        ("sp400",        wikipedia_url_sp400),
        ("sp600",        wikipedia_url_sp600),
        ("ishares_core", ishares_url, ...),
    ]
    
    pipeline:
        for each source:
            rows = scrape(url)
            tag rows with source column
        merged = concat(all) → deduplicate on ticker
        
        if preserve_manual:
            existing_manual = load_csv()[is_manual]
            merged = concat([merged, existing_manual]).drop_duplicates('ticker')
        
        merged.to_csv('csv/tradable_candidates.csv')
```

---

## CSV Schema

### tradable_candidates.csv

| 列名 | 类型 | 说明 |
|------|------|------|
| ticker | str | 交易代码（大写） |
| name | str | 公司名称 |
| exchange | str | 交易所（NASDAQ/NYSE/AMEX） |
| asset_type | str | Stock / ETF |
| source | str | russell_1000 / sp500 / manual 等 |

### all_us_stocks_listed.csv

| 列名 | 类型 | 说明 |
|------|------|------|
| ticker | str | 交易代码 |
| exchange | str | 交易所 |
| assetType | str | Stock / ETF / Fund |
| priceCurrency | str | 通常为 USD |
| startDate | date | 上市日期 |
| endDate | date | NULL = 仍在市 |

---

## Configuration Parameters

从 `config.yaml` 读取（通过 `DEFAULT_PRICE_FLOOR()`, `DEFAULT_PRICE_CEILING()` 等工具函数）：

```yaml
price:
  floor: 5.0        # 最低股价阈值
  ceiling: 10000.0  # 最高股价阈值（过滤 Berkshire Class A 等极高价股）
  jump_threshold: 0.95
  ratio_limit: 10
```

---

## Constitution Check

| 原则 | 状态 | 说明 |
|------|------|------|
| II. 增量更新 | ✅ | 每日根据最新数据重新评估 is_tradable |
| IV. 配置集中化 | ✅ | price_floor 来自 config.yaml（已修复原 Bug #2）|
| VIII. 简洁优先 | ✅ | update_tradable_universe 单一职责 |

---

## Known Issues & Future Work

| 问题 | 状态 | 计划 |
|------|------|------|
| volume_floor 阈值未在 config.yaml 中配置 | ⚠️ 待确认 | 确认是否启用美元成交量过滤 |
| 退市检测依赖 delist_date 字段，填充不完整 | ⚠️ 待改进 | 接入 Tiingo 退市数据或定期比对 |
| 行业数据填充（fill_sector_industry）手动触发 | ⚠️ 手动 | 方向 E3 中纳入自动化 |
