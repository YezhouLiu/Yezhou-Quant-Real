# =============================================================================
# Yezhou Capital Limited  |  Proprietary & Confidential
# =============================================================================
# Copyright (c) 2026 Yezhou Capital Limited. All rights reserved.
#
# Project  : Yezhou Quantitative Trading System
# Author   : Yezhou Liu
# Contact  : yezhoucapital@gmail.com
#
# This source code is the exclusive property of Yezhou Capital Limited.
# Unauthorized copying, modification, distribution, or use of this file,
# via any medium, is strictly prohibited without prior written consent.
# =============================================================================
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import requests
from time import sleep
from typing import List, Optional, Dict
from datetime import datetime
from database.utils.db_utils import get_db_connection
from utils.config_loader import get_config_value
from utils.logger import get_logger

log = get_logger("update_universe")


# ----------------------------------------------------------------------------------------------------------------------------------------
# 从 CSV 读取一列
# ----------------------------------------------------------------------------------------------------------------------------------------
def read_tickers_from_csv(csv_path: Path, column_name: str = "ticker") -> List[str]:
    if not csv_path.exists():
        log.error(f"❌ 文件不存在: {csv_path}")
        return []
    
    try:
        df = pd.read_csv(csv_path)
        if column_name not in df.columns:
            log.error(f"❌ 列 '{column_name}' 不存在")
            return []
        
        return df[column_name].dropna().astype(str).str.strip().unique().tolist()
    except Exception as e:
        log.error(f"❌ 读取失败: {e}")
        return []


# ----------------------------------------------------------------------------------------------------------------------------------------
# 从 Tiingo Meta API 获取元数据
# ----------------------------------------------------------------------------------------------------------------------------------------
def fetch_tiingo_meta(ticker: str, api_token: str, session: requests.Session) -> Optional[Dict]:
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}"
    headers = {'Content-Type': 'application/json', 'Authorization': f'Token {api_token}'}
    try:
        response = session.get(url, headers=headers, timeout=10)
        return response.json() if response.status_code == 200 else None
    except:
        return None


# ----------------------------------------------------------------------------------------------------------------------------------------
# 从 Tiingo 填充详细信息
# ----------------------------------------------------------------------------------------------------------------------------------------
def enrich_from_tiingo(conn, session: requests.Session, tickers: Optional[List[str]] = None, force: bool = False) -> Dict[str, int]:
    api_token = get_config_value("tiingo.api_key")
    if not api_token:
        log.error("❌ 未配置 Tiingo API Token (请在 secrets.env 设置 TIINGO_API_KEY)")
        return {"success": 0, "failed": 0}
    
    cursor = conn.cursor()
    
    if force:
        # 强制更新所有
        if tickers:
            cursor.execute("SELECT ticker FROM instruments WHERE ticker = ANY(%s)", (tickers,))
        else:
            cursor.execute("SELECT ticker FROM instruments")
    else:
        # 只更新未拉取过的（ipo_date IS NULL 表示从未拉取）
        if tickers:
            cursor.execute("SELECT ticker FROM instruments WHERE ticker = ANY(%s) AND ipo_date IS NULL", (tickers,))
        else:
            cursor.execute("SELECT ticker FROM instruments WHERE ipo_date IS NULL")
    
    to_update = [row[0] for row in cursor.fetchall()]
    
    if not to_update:
        log.info("⏭️  无需更新")
        return {"success": 0, "failed": 0}
    
    log.info(f"🔄 填充 {len(to_update)} 个 tickers...")
    
    success, failed = 0, 0
    for i, ticker in enumerate(to_update, 1):
        meta = fetch_tiingo_meta(ticker, api_token, session)
        if not meta:
            failed += 1
            continue
        
        name = meta.get("name")
        desc = meta.get("description")
        exchange = meta.get("exchangeCode") or "UNKNOWN"
        ipo = None
        if meta.get("startDate"):
            try:
                ipo = datetime.fromisoformat(meta["startDate"].replace('Z', '+00:00')).date()
            except:
                pass
        
        cursor.execute("""
            UPDATE instruments
            SET company_name = %s, description = %s, exchange = %s, ipo_date = %s, updated_at = now()
            WHERE ticker = %s
        """, (name, desc, exchange, ipo, ticker))
        
        success += 1
        log.info(f"[{i}/{len(to_update)}] ✅ {ticker}: {name}")
        sleep(0.2)
    
    conn.commit()
    log.info(f"✅ 完成: 成功 {success}, 失败 {failed}")
    return {"success": success, "failed": failed}


# ----------------------------------------------------------------------------------------------------------------------------------------
# 完整流程：更新所有 instruments
# ----------------------------------------------------------------------------------------------------------------------------------------
def update_all_instruments():
    csv_dir = Path(get_config_value("path.csv_dir"))
    
    # 读取 ETF
    etf_tickers = read_tickers_from_csv(csv_dir / "etf.csv")
    # 读取股票
    stock_tickers = read_tickers_from_csv(csv_dir / "tradable_candidates.csv")
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # 插入 ETF
        for t in etf_tickers:
            cursor.execute("""
                INSERT INTO instruments (ticker, asset_type, is_tradable)
                SELECT %s, 'ETF', false
                WHERE NOT EXISTS (SELECT 1 FROM instruments WHERE ticker = %s)
            """, (t, t))
        
        # 插入股票
        for t in stock_tickers:
            cursor.execute("""
                INSERT INTO instruments (ticker, asset_type, is_tradable)
                SELECT %s, 'Stock', true
                WHERE NOT EXISTS (SELECT 1 FROM instruments WHERE ticker = %s)
            """, (t, t))
        
        conn.commit()
        log.info(f"✅ 插入完成")
        
        # 填充详细信息
        session = requests.Session()
        try:
            enrich_from_tiingo(conn, session)
        finally:
            session.close()


if __name__ == "__main__":
    update_all_instruments()

