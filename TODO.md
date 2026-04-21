# 项目 TODO & 扩展路线图

> 现状评估 + 未来方向（慢量化 · 价值投资视角）
> 最后更新：2026-04-07

---

## 一、项目现状总结

### 已完成 ✅

| 模块 | 状态 |
|---|---|
| PostgreSQL 数据库 + 13张表的 schema | ✅ 完整 |
| Tiingo EOD 日线价格增量下载 | ✅ 运行中 |
| SEC EDGAR XBRL 基本面数据下载 | ✅ 可运行，quarterly 触发 |
| 可交易宇宙动态更新（价格/成交量过滤） | ✅ 运行中 |
| 6 个技术因子（动量/波动率/回撤/跳跃风险等） | ✅ 完整 |
| 回测引擎（Strategy-Scorer-Selector-Portfolio 流水线） | ✅ 可运行 |
| 月度再平衡逻辑 | ✅ 可运行 |
| 交易摩擦成本模型（滑点/手续费/资金保留比例） | ✅ 完整 |
| 回测结果写入 `exp_positions`，NAV 可视化 | ✅ 基本可用 |

### 已有架构但尚未激活 ⚠️

| 模块 | 问题 |
|---|---|
| `fundamental_data` 表 | 数据已下载，**但没有任何因子或策略读取它** |
| `fundamental_daily` 表（日度 PE/PB） | 表结构和 RW 方法存在，**没有下载器向其写入数据** |
| `fills` / `positions` 表（实盘追踪） | 表存在，**从未写入，系统仍是纯研究模式** |
| 公司行为（股票拆分/合并） | `extract_corporate_actions()` 被注释掉，**未处理** |
| `is_factor_enabled` 字段 | `instruments` 表有此列，因子计算脚本实际未使用 |

### 已知 Bug 🐛

1. **`rw_fundamental_data.py` 列名与 DDL 不匹配** — ~~RW 方法用的是 `as_of_date`/`value`，实际表里是 `report_date`/`metric_value`；写基本面数据会报错。~~ **搁置**：Tiingo 未购买基本面授权，SEC EDGAR 链路尚未跑通，等实际有数据写入需求时再修。
2. **`price_floor` 不一致** — ~~`config.yaml` 设置为 $1.5，但 `update_tradable_universe()` 里硬编码为 $5。~~ ✅ **已修复**：`config.yaml` 改为 $5.0，`update_tradable_universe()` 现在读取 `DEFAULT_PRICE_FLOOR()` 而非硬编码。
3. **Corporate Actions 缺失** — ~~历史拆股事件没有重新调整因子计算的基准，长回测中会有逻辑错误。~~ **不是 Bug**：因子和 NAV 均基于 Tiingo 提供的前复权价格 (`adj_close`)，拆股已在数据源层消化，`extract_corporate_actions()` 仅用于记录事件流水，不影响回测准确性。

---

## 二、短期 Bug 修复（先于一切扩展）

- [ ] ~~修复 `rw_fundamental_data.py` 的列名映射（`as_of_date` → `report_date`，`value` → `metric_value`）~~ **搁置**，等有实际基本面数据写入需求时再处理
- [x] 统一 `price_floor`：`config.yaml` 改为 $5.0，`update_tradable_universe()` 改为读取 config 而非硬编码 ✅
- [ ] ~~验证 SEC EDGAR 下载后数据能否成功写入数据库并查询出来~~ **搁置**，与上同
- [ ] ~~激活公司行为管道 —— 至少做拆股调整，否则长期回测 NAV 曲线失真~~ **不是 Bug**：Tiingo `adj_close` 已前复权，回测 NAV 不受拆股影响

---

## 三、扩展方向

---

### 方向 A：基本面因子（核心价值投资信号）

> **这是整个项目最大的未开垦金矿。** SEC EDGAR 的数据已经下来了，却没有用到任何一个因子里。

#### A1 · 估值因子

- [ ] **市盈率倒数（E/P，Earnings Yield）** — 用 TTM EPS × stock price 计算，低 PE = 高 EP = 便宜，做多高EP股票
- [ ] **市净率（P/B）的倒数** — 经典 Fama-French HML 因子的核心
- [ ] **市销率（P/S）的倒数** — 对无盈利成长股也适用，补充 EP
- [ ] **EV/EBITDA 倒数** — 比 PE 更「去杠杆化」的估值指标，需要负债数据
- [ ] **自由现金流收益率（FCF/P）** — 「真金白银」的价值信号，比会计利润更难造假
- [ ] **股息率（Dividend Yield）** — 稳定分红是价值企业「硬指标」，SEC/Tiingo 均可拿到

#### A2 · 质量因子（筛选出「好便宜」而非「烂便宜」）

- [ ] **ROE / ROIC** — 资本回报率；长期 ROIC > WACC 是护城河标志
- [ ] **毛利率稳定性** — 高且稳定的毛利率 = 定价权
- [ ] **Piotroski F-Score**（9个维度的会计健康分） — 价值陷阱过滤神器，具体维度：
  - 净利润为正、经营性现金流为正、ROA 同比改善
  - 经营性现金流 > 净利润（利润质量）
  - 长期债务比率下降、流动比率上升、未发行新股
  - 毛利率改善、资产周转率改善
- [ ] **Sloan 应计比率（Accruals Ratio）** — 应计利润占比越高，未来收益越可能下修；反向因子

#### A3 · 成长因子（GARP：合理价位的成长）

- [ ] **EPS 同比增速（YoY）** — 季报 vs 去年同期
- [ ] **收入同比增速** — 顶线增长
- [ ] **PEG Ratio** — PE / EPS增速，格雷厄姆传人彼得·林奇发扬，PEG < 1 为低估

---

### 方向 B：回测分析体系升级

> 目前回测只输出 NAV 曲线，没有任何量化性能指标，无法判断策略是否真的有 alpha。

- [ ] **核心绩效指标**
  - Sharpe Ratio、Sortino Ratio、Calmar Ratio
  - 最大回撤（Max Drawdown）和恢复期（Recovery Period）
  - 年化收益 vs 年化波动

- [ ] **超额收益分析**
  - Alpha（相对 SPY/IWM 的年化超额）
  - Beta、Information Ratio（IR）

- [ ] **因子有效性验证**
  - IC（Information Coefficient）：每月因子值 vs 下月收益的 Spearman 相关系数
  - IC 均值、ICIR（IC / std(IC)）
  - 分位数收益分析（五分位回测：Q1 vs Q5 的月均收益差）

- [ ] **持仓分析**
  - 月度换手率（Turnover）
  - 持仓集中度（HHI）
  - 行业/板块暴露度

- [ ] **可导出报告** —— 生成 HTML/PDF 回测摘要（参考 quantstats 库）

---

### 方向 C：投资组合构建升级

> 目前只有等权重（Equal Weight），这对价值投资来说太粗糙了。

- [ ] **按估值倒数加权** —— 比如用 E/P 加权，最便宜的股票拿最大仓位
- [ ] **风险平价（Risk Parity）** —— 按波动率倒数加权，让每只股票贡献相同的风险
- [ ] **最大分散化（Max Diversification）** —— 最大化各持仓间的相关性分散
- [ ] **均值-方差优化（MVO）** —— 经典 Markowitz，建议用 `cvxpy` + 收缩估计量（Ledoit-Wolf）
- [ ] **行业中性** —— 强制每个 GICS 板块不超过 X%，避免不自觉地押注单一行业（比如全能源）
- [ ] **单只股票仓位上限** —— 无论打分多高，单票上限 15–20%

---

### 方向 D：价值投资专项策略

> 以下策略可直接实现为新的 `ScoringStrategy` 实例，插入现有引擎。

#### D1 · 股息成长策略（Dividend Growth Investing）
类似 David Fish "CCC"（Dividend Champions/Contenders/Challengers）的量化版：
- 筛选条件：连续 N 年增加股息（`N ≥ 5`）
- 评分因子：股息增速 × 股息率 × Piotroski F-Score
- 目标：稳定现金流 + 复利积累，适合不想择时的长期持有

#### D2 · 深度价值策略（Net-Net / Graham Number）
- **Net-Net**：股价 < (流动资产 - 总负债) / 股数；格雷厄姆 1930s 原版，现代市场稀少但暴跌后出现
- **Graham Number**：$\sqrt{22.5 \times \text{EPS} \times \text{BVPS}}$，股价 < Graham Number 即为低估
- 适合熊市下跌后扫货

#### D3 · 高质量低估值策略（Quality + Value，Buffett 风格）
- 筛选：ROIC > 15% AND F-Score ≥ 7 (高质量)
- 排序：E/P + FCF/P 综合估值分（买便宜的好公司）
- 持仓：10–15 只，季度再平衡

#### D4 · 逆向投资策略（Contrarian / Mean-Reversion）
- 买入过去 12 个月跌幅最大 + F-Score 高的股票（基本面健康但被错杀）
- 12 个月持有，然后再平衡（利用价值回归）
- 参考「52 周低点效应」研究

#### D5 · 因子轮动策略（Factor Rotation）
- 跟踪价值因子（HML）和质量因子（QMJ）的相对强弱
- 当价值因子相对动量好时，加大 E/P 权重；否则转向质量因子
- 可用 Fama-French 数据库验证历史效果

---

### 方向 E：数据扩充

#### E1 · 基本面日度化（已有表结构，需实现下载器）
- [ ] `fundamental_daily` 表：每日用最新报告数据 × 当日股价，计算 PE/PB/PS
- [ ] 这样估值因子就能每天更新，而不是只在季报日更新一次

#### E2 · 股息数据
- [ ] Tiingo 的 EOD 数据里已有 `divCash` 字段（split-adjusted），提取并存入 `corporate_actions` 表
- [ ] 计算 trailing 12-month dividend yield = sum(dividends in past 252 days) / price

#### E3 · 行业/板块数据完善
- [ ] `fill_sector_industry_yfinance.py` 已存在，跑一遍把所有 instruments 的 `sector`/`industry` 填满
- [ ] 这是行业中性策略的前提

#### E4 · 财报日历（Earnings Calendar）
- [ ] 接入 Yahoo Finance / Alpha Vantage earnings calendar API
- [ ] 在再平衡时规避「即将发布财报」的股票（避免财报雷）
- [ ] 或者反过来：在财报後超预期下跌时买入（earnings-dip 策略）

#### E5 · 内部人交易数据（Insider Buying）
- [ ] SEC Form 4 是公开免费的，可以通过 `data.sec.gov/submissions` 获取
- [ ] CEO/CFO 买入高管自家股票 = 强烈看涨信号（尤其是小盘价值股）

---

### 方向 F：运维与工程

> 慢量化不需要毫秒级系统，但需要「可靠的日常运行」和「结果可观测」。

- [ ] **定时任务调度** —— 用 `APScheduler` 或 Windows Task Scheduler 实现：
  - 每个交易日收盘后 8pm 自动运行 `daily_update()`
  - 每季度末自动运行 `seasonal_update()`
  - 月末账单日自动生成再平衡建议单

- [ ] **再平衡建议单（Order Sheet）** —— 月度再平衡时，系统输出：
  - 当前持仓 → 目标持仓的差异
  - 需要买入/卖出的股票 + 金额（我手动执行）
  - 估算交易摩擦成本

- [ ] **邮件提醒** —— 月末自动发邮件：「本月再平衡建议，请查收」
- [ ] **实盘仓位追踪** —— 把 Interactive Brokers / 券商 CSV 导出的持仓手动录入 `positions` 表，与 `exp_positions` 对比了解跟踪误差

- [ ] **数据质量监控** —— 每日下载后检查：异常跳空>30%、数据缺失天数、NA 占比

---

### 方向 G：Web Dashboard

> 目前只有本地 matplotlib 图，难以分享和每天查看。

- [ ] 用 **Streamlit**（最快）或 FastAPI + Plotly 搭建本地 dashboard
- [ ] 页面1：投资组合 NAV vs SPY/QQQ，过去 1M/3M/1Y/All（参考 quantstats）
- [ ] 页面2：当前「推荐持仓」—— 今日打出的 top-K 个股，附估值、动量、质量分
- [ ] 页面3：每只当前持股的基本面摘要（PE、PB、FCF Yield、ROE、EPS增速）
- [ ] 页面4：因子监控 —— 最近 6 个月的IC序列折线图（了解哪些因子正在失效）
- [ ] 页面5：宇宙卫生检查 —— 多少只股票有数据缺失？数据最久的/最新的日期？

---

### 方向 H：研究实验 / 量化内功

> 这些是「论文层面」的方向，可以慢慢做，也最有可能产出真正的 alpha。

- [ ] **超宇宙分析** —— 把整个可交易宇宙的 PE/PB/FCF Yield 的历史分布画出来，判断当前市场整体贵不贵（择时辅助）
- [ ] **因子拥挤度监测** —— 如果「低 PE」策略近年收益大跌，可能是太多人在做，信号失效
- [ ] **Fama-French 五因子对标** —— 把自己策略的收益用 FF5 分解，看 alpha 是否真的不可解释
- [ ] **尾部风险分析** —— Monte Carlo 模拟 1000 条 NAV 路径，量化「一年内跌 30%」的概率
- [ ] **参数稳健性测试** —— 换持仓数量（top3 vs top10 vs top20）、换再平衡频率（月/季/半年），NAV 有多大波动？
- [ ] **实盘 vs 回测偏差跟踪** —— 6 个月后把实盘收益和同期回测收益对比，找出滑点/时机/数据偏差的来源

---

## 四、优先级建议

根据「性价比」排序（效果/工作量）：

| 优先级 | 方向 | 理由 |
|---|---|---|
| 🔴 P0 | ✅ `price_floor` 统一（config $5.0 + 读 config） | 已完成 |
| 🔴 P0 | A1 基本面估值因子（E/P, FCF/P） | 数据已有，代码量小，对价值投资意义最大 |
| 🟠 P1 | B 回测分析体系（Sharpe/IC/分位数）| 没有这个，根本不知道策略是否有效 |
| 🟠 P1 | E1+E2 基本面日度化 + 股息数据 | 让估值因子天天更新 |
| 🟡 P2 | A2 质量因子（Piotroski F-Score） | 避免价值陷阱，是估值因子的最佳搭档 |
| 🟡 P2 | D1 股息成长策略 | 对长期持有者最自然的策略 |
| 🟡 P2 | E3 行业数据填充 → C 行业中性 | 防止隐性行业押注 |
| 🟢 P3 | F 定时任务 + 再平衡建议单 | 工程化，让系统真正「跑起来」 |
| 🟢 P3 | G Web Dashboard（Streamlit） | 可观测性，提升使用体验 |
| 🔵 P4 | D2–D5 专项策略 | 需要 P0/P1 完成后才值得研究 |
| 🔵 P4 | H 研究实验 | 长期内功，随时可做 |

---

## 五、一句话愿景

> 用系统性的基本面筛选 + 量化纪律，找到被市场低估的优质美股，
> 每月检视一次，每季度再平衡一次，
> 让时间和复利做大部分工作。

---

*本文件由 GitHub Copilot 辅助生成，供项目负责人参考扩展思路，实际方向以个人判断为准。*
