import sys
import re
from io import StringIO
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import requests
from datetime import date
from utils.config_loader import get_config_value


# ----------------------------------------------------------------------------------------------------------------------------------------
# Ticker 验证
# ----------------------------------------------------------------------------------------------------------------------------------------
_VALID_TICKER_PATTERN = re.compile(r'^[A-Z]{1,5}([.-][A-Z]{1,2})?$')

def is_valid_us_ticker(ticker: str) -> bool:
    """验证是否为合法的美股 ticker"""
    if not ticker:
        return False
    return bool(_VALID_TICKER_PATTERN.fullmatch(ticker))


# ----------------------------------------------------------------------------------------------------------------------------------------
# S&P 500 当前成分（保留用于 Growth Radar 验证）
# ----------------------------------------------------------------------------------------------------------------------------------------
def fetch_sp500_list() -> list[tuple[str, str, str, date]]:
    """抓取 S&P 500 成分股（仅用于验证，不作为主 universe）"""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    tables = pd.read_html(url, storage_options=headers)
    df = tables[0]
    tickers = df["Symbol"].astype(str).str.replace(".", "-", regex=False)
    names = df["Security"].astype(str)
    return list(
        zip(tickers, names, ["sp500"] * len(tickers), [date.today()] * len(tickers))
    )


# ----------------------------------------------------------------------------------------------------------------------------------------
# NASDAQ-100 当前成分（保留用于 Growth Radar - 科技成长股专用）
# ----------------------------------------------------------------------------------------------------------------------------------------
def fetch_nasdaq100_list() -> list[tuple[str, str, str, date]]:
    """抓取 NASDAQ-100 成分股（用于 Growth Radar 科技股识别）"""
    url = "https://en.wikipedia.org/wiki/NASDAQ-100"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    tables = pd.read_html(url, storage_options=headers)
    df = None
    for table in tables:
        if "Ticker" in table.columns and "Company" in table.columns:
            df = table
            break
    if df is None:
        raise ValueError("找不到含有 NASDAQ-100 股票的表格")
    tickers = df["Ticker"].astype(str).str.replace(".", "-", regex=False)
    names = df["Company"].astype(str)
    return list(
        zip(tickers, names, ["nasdaq100"] * len(tickers), [date.today()] * len(tickers))
    )


# ----------------------------------------------------------------------------------------------------------------------------------------
# S&P 400 MidCap 当前成分（保留 - 完整候选池）
# ----------------------------------------------------------------------------------------------------------------------------------------
def fetch_sp400_list() -> list[tuple[str, str, str, date]]:
    """抓取 S&P 400 MidCap 成分股（完整候选池，默认不激活）"""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    tables = pd.read_html(url, storage_options=headers)
    df = tables[0]
    
    # 灵活查找 ticker 列（Symbol 或 Ticker）
    ticker_col = 'Symbol' if 'Symbol' in df.columns else 'Ticker' if 'Ticker' in df.columns else df.columns[0]
    # 灵活查找公司名列（Security 或 Company 或 Company Name）
    name_col = 'Security' if 'Security' in df.columns else 'Company' if 'Company' in df.columns else 'Company Name' if 'Company Name' in df.columns else df.columns[1]
    
    tickers = df[ticker_col].astype(str).str.replace(".", "-", regex=False)
    names = df[name_col].astype(str)
    return list(
        zip(tickers, names, ["sp400"] * len(tickers), [date.today()] * len(tickers))
    )


# ----------------------------------------------------------------------------------------------------------------------------------------
# S&P 600 SmallCap 当前成分（保留 - 完整候选池）
# ----------------------------------------------------------------------------------------------------------------------------------------
def fetch_sp600_list() -> list[tuple[str, str, str, date]]:
    """抓取 S&P 600 SmallCap 成分股（完整候选池，默认不激活）"""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    tables = pd.read_html(url, storage_options=headers)
    df = tables[0]
    
    # 灵活查找 ticker 列（Symbol 或 Ticker）
    ticker_col = 'Symbol' if 'Symbol' in df.columns else 'Ticker' if 'Ticker' in df.columns else df.columns[0]
    # 灵活查找公司名列（Company 或 Security 或 Company Name）
    name_col = 'Company' if 'Company' in df.columns else 'Security' if 'Security' in df.columns else 'Company Name' if 'Company Name' in df.columns else df.columns[1]
    
    tickers = df[ticker_col].astype(str).str.replace(".", "-", regex=False)
    names = df[name_col].astype(str)
    return list(
        zip(tickers, names, ["sp600"] * len(tickers), [date.today()] * len(tickers))
    )


# ----------------------------------------------------------------------------------------------------------------------------------------
# ✅ Russell 1000 - 主战场 Universe（Core Layer）
# 覆盖美国大盘+中盘，~90% 市值，~1000 只股票
# 这是 PM 级策略的主战场，不靠委员会筛选，只看市值排名
# ----------------------------------------------------------------------------------------------------------------------------------------
def fetch_russell1000_list() -> list[tuple[str, str, str, date]]:
    """
    抓取 Russell 1000 成分股（主 Universe）
    
    数据来源：iShares Russell 1000 ETF (IWB) 持仓
    注意：iShares 提供的是 ETF 持仓，与指数成分可能有微小差异，但对研究影响很小
    """
    url = "https://www.ishares.com/us/products/239707/ishares-russell-1000-etf/1467271812596.ajax?fileType=csv"
    response = requests.get(url)
    text = response.text
    lines = text.splitlines()
    
    # 找到表头行
    for i, line in enumerate(lines):
        if "Ticker" in line or "Symbol" in line:
            header_index = i
            break
    else:
        raise ValueError("❌ 找不到包含 Ticker 的表头行")
    
    df = pd.read_csv(StringIO("\n".join(lines[header_index:])))
    
    # 识别 ticker 和 name 列
    ticker_col = next(
        (c for c in df.columns if c in ["Ticker", "Symbol", "Ticker Symbol"]), None
    )
    name_col = next((c for c in df.columns if "Name" in c or "Company" in c), None)

    if ticker_col is None or name_col is None:
        raise ValueError(
            f"❌ 未能识别 Ticker/Name 列，实际列为: {', '.join(df.columns)}"
        )
    
    tickers = df[ticker_col].astype(str).str.replace(".", "-", regex=False)
    names = df[name_col].astype(str)
    
    return list(
        zip(
            tickers,
            names,
            ["russell1000"] * len(tickers),
            [date.today()] * len(tickers),
        )
    )


# ----------------------------------------------------------------------------------------------------------------------------------------
# 🔍 Russell 2000 - Growth Radar 原料池（Exploration Layer）
# 用于识别"正在变大的鱼"，但需要过滤后使用
# ----------------------------------------------------------------------------------------------------------------------------------------
def fetch_russell2000_list() -> list[tuple[str, str, str, date]]:
    """
    抓取 Russell 2000 成分股（Growth Radar 原料）
    
    ⚠️ 不直接作为主 Universe，而是用于：
    - 筛选市值 Top 20% 的小盘突破股
    - 识别高成长潜力标的
    - 探索性策略研究
    """
    url = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv"
    response = requests.get(url)
    text = response.text
    lines = text.splitlines()
    
    for i, line in enumerate(lines):
        if "Ticker" in line or "Symbol" in line:
            header_index = i
            break
    else:
        raise ValueError("❌ 没找到 Ticker 列所在的表头行")
    
    df = pd.read_csv(StringIO("\n".join(lines[header_index:])))
    ticker_col = next(
        (c for c in df.columns if c in ["Ticker", "Symbol", "Ticker Symbol"]), None
    )
    name_col = next((c for c in df.columns if "Name" in c or "Company" in c), None)
    
    if ticker_col is None or name_col is None:
        raise ValueError(
            f"❌ 未能识别 Ticker/Name 列，实际列为: {', '.join(df.columns)}"
        )
    
    tickers = df[ticker_col].astype(str).str.replace(".", "-", regex=False)
    names = df[name_col].astype(str)
    
    return list(
        zip(tickers, names, ["russell2000"] * len(tickers), [date.today()] * len(tickers))
    )


# ----------------------------------------------------------------------------------------------------------------------------------------
# ✅ 保存候选股票池到 CSV（支持手动编辑 + 保留手动添加）
# 
# 核心逻辑：
# 1. 爬取所有来源的股票（SP500/400/600 + Russell 1000/2000 + NASDAQ-100）
# 2. 保留 CSV 中已有的手动添加股票（source='manual_*'）
# 3. 去重后保存（ticker 为主键）
# 4. 支持手动编辑 CSV 添加 OpenAI, SpaceX 等历史级 IPO
# ----------------------------------------------------------------------------------------------------------------------------------------
def save_tradable_candidates_csv(
    csv_path: Path = Path(get_config_value("path.csv_dir")) / "tradable_candidates.csv",
    include_all_sources: bool = True,  # 是否包含所有来源（SP400/600/Russell2000）
    preserve_manual: bool = True,      # 是否保留手动添加的股票
):
    """
    保存候选股票池到 CSV（支持手动编辑）
    
    Args:
        csv_path: CSV 保存路径
        include_all_sources: 是否包含所有来源（True=完整候选池，False=只 Russell 1000）
        preserve_manual: 是否保留手动添加的股票（source='manual_*'）
    
    核心特性：
    - ✅ 爬取最新成分股
    - ✅ 保留手动添加的股票（OpenAI, SpaceX 等）
    - ✅ CSV 可手动编辑（Excel/VSCode）
    - ✅ 防止频繁爬虫被封
    
    CSV 格式：
    ticker,company_name,source,added_at,is_active_default,notes
    AAPL,Apple Inc.,russell1000,2024-01-01,FALSE,
    OPENAI,OpenAI Inc.,manual_ipo,2026-01-15,TRUE,Sam Altman AGI company
    """
    print("🔄 开始更新候选股票池 CSV...")
    
    # 步骤 1: 保留手动添加的股票
    existing_manual = []
    if csv_path.exists() and preserve_manual:
        print("\n📖 读取现有 CSV，保留手动添加...")
        try:
            existing_df = pd.read_csv(csv_path)
            # 保留所有 source 以 'manual' 开头的记录
            if 'source' in existing_df.columns:
                manual_mask = existing_df['source'].astype(str).str.startswith('manual')
                manual_df = existing_df[manual_mask]
                existing_manual = manual_df.to_dict('records')
                print(f"   ✅ 找到 {len(existing_manual)} 只手动添加的股票")
        except Exception as e:
            print(f"   ⚠️  读取现有 CSV 失败: {e}")
    
    # 步骤 2: 爬取最新数据
    print("\n� 爬取最新成分股...")
    
    # 核心：Russell 1000（必须）
    print("   [Core] Russell 1000...")
    russell1000 = fetch_russell1000_list()
    print(f"      ✅ {len(russell1000)} 只股票")
    
    all_crawled = russell1000
    
    if include_all_sources:
        # 完整候选池：所有来源
        print("   [Full] S&P 500...")
        sp500 = fetch_sp500_list()
        print(f"      ✅ {len(sp500)} 只股票")
        
        print("   [Full] S&P 400 MidCap...")
        sp400 = fetch_sp400_list()
        print(f"      ✅ {len(sp400)} 只股票")
        
        print("   [Full] S&P 600 SmallCap...")
        sp600 = fetch_sp600_list()
        print(f"      ✅ {len(sp600)} 只股票")
        
        print("   [Full] NASDAQ-100...")
        nasdaq100 = fetch_nasdaq100_list()
        print(f"      ✅ {len(nasdaq100)} 只股票")
        
        print("   [Full] Russell 2000...")
        russell2000 = fetch_russell2000_list()
        print(f"      ✅ {len(russell2000)} 只股票")
        
        all_crawled = sp500 + sp400 + sp600 + russell1000 + nasdaq100 + russell2000
    
    # 步骤 3: 合并爬虫数据和手动数据
    print("\n� 合并数据...")
    
    # 将爬虫数据转为 DataFrame
    df_crawled = pd.DataFrame(all_crawled, columns=["ticker", "company_name", "source", "added_at"])
    df_crawled['is_active_default'] = False  # 爬虫数据默认不激活
    df_crawled['notes'] = ''
    
    # 将手动数据转为 DataFrame
    if existing_manual:
        df_manual = pd.DataFrame(existing_manual)
        # 确保列一致
        for col in ['ticker', 'company_name', 'source', 'added_at', 'is_active_default', 'notes']:
            if col not in df_manual.columns:
                df_manual[col] = '' if col == 'notes' else None
        
        # 合并
        df_combined = pd.concat([df_crawled, df_manual], ignore_index=True)
    else:
        df_combined = df_crawled
    
    # 步骤 4: 过滤非法 ticker
    before_filter = len(df_combined)
    df_combined = df_combined[df_combined['ticker'].apply(is_valid_us_ticker)]
    after_filter = len(df_combined)
    if before_filter > after_filter:
        print(f"\n🧹 过滤非法 ticker: 移除 {before_filter - after_filter} 条")
    
    # 步骤 5: 去重（ticker 为主键，保留第一次出现）
    df_combined = df_combined.drop_duplicates(subset=['ticker'], keep='first')
    
    # 步骤 6: 保存
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df_combined.to_csv(csv_path, index=False)
    print(f"\n✅ 成功写入 {len(df_combined)} 条有效记录到: {csv_path}")
    
    # 统计信息
    crawled_count = len(df_crawled)
    manual_count = len(existing_manual)
    total_count = len(df_combined)
    
    print(f"\n✅ 候选股票池已更新")
    print(f"   📊 爬虫数据: {crawled_count} 只")
    print(f"   ✍️  手动添加: {manual_count} 只")
    print(f"   📈 去重后总计: {total_count} 只")
    print(f"   📂 保存至: {csv_path}")
    
    print(f"\n💡 提示：")
    print(f"   - 可手动编辑 CSV 添加 OpenAI, SpaceX 等历史级 IPO")
    print(f"   - source='manual_ipo' 的股票会在下次更新时保留")
    print(f"   - is_active_default=TRUE 的股票导入数据库时会自动激活")


# ----------------------------------------------------------------------------------------------------------------------------------------
# 测试入口
# ----------------------------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    # 完整版：所有来源（推荐）
    save_tradable_candidates_csv(include_all_sources=True)
    
    # 如果只想要 Russell 1000（快速测试）：
    # save_tradable_candidates_csv(include_all_sources=False)
    
    print("\n💡 下一步：")
    print("   1. 检查 CSV 文件，确认数据正确")
    print("   2. 可手动编辑 CSV 添加 OpenAI, SpaceX 等股票")
    print("   3. 运行 python tasks/import_instruments_from_csv.py 导入数据库")
