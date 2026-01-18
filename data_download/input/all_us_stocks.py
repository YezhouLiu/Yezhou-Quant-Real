"""
从 Tiingo API 下载官方支持的股票列表
Tiingo 每日更新 supported_tickers.zip 文件，包含所有可交易的股票代码
数据源：https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip
优势：
- 官方数据，准确可靠
- 每日更新
- 包含 Tiingo 实际支持的所有股票
- 无需过滤，避免误判
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import zipfile
import pandas as pd
import requests
from io import BytesIO
from datetime import date
from typing import Optional
from utils.config_loader import get_config_value
from utils.logger import get_logger

logger = get_logger(__name__)


# ----------------------------------------------------------------------------------------------------------------------------------------
# 从 Tiingo 下载支持的股票列表
# ----------------------------------------------------------------------------------------------------------------------------------------
def download_tiingo_supported_tickers() -> Optional[pd.DataFrame]:
    """
    从 Tiingo API 下载官方支持的股票列表
    
    Returns:
        包含股票信息的 DataFrame，失败返回 None
        
    API 说明：
        - URL: https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip
        - 格式: ZIP 压缩的 CSV 文件
        - 更新频率: 每日更新
        - 无需 API Token
    """
    url = "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"
    
    try:
        logger.info(f"📥 开始下载 Tiingo 股票列表: {url}")
        
        # 下载 ZIP 文件
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        logger.info(f"✅ 下载成功，文件大小: {len(response.content) / 1024:.1f} KB")
        
        # 解压 ZIP 文件
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            # 获取 ZIP 中的第一个文件（通常是 supported_tickers.csv）
            csv_filename = z.namelist()[0]
            logger.info(f"📂 解压文件: {csv_filename}")
            
            # 读取 CSV
            with z.open(csv_filename) as csv_file:
                df = pd.read_csv(csv_file)
        
        logger.info(f"✅ 成功解析股票列表，共 {len(df)} 条记录")
        logger.debug(f"列名: {df.columns.tolist()}")
        
        return df
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ 网络请求失败: {e}")
        return None
    except zipfile.BadZipFile as e:
        logger.error(f"❌ ZIP 文件损坏: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ 下载失败: {e}")
        return None


# ----------------------------------------------------------------------------------------------------------------------------------------
# 处理和标准化股票列表数据
# ----------------------------------------------------------------------------------------------------------------------------------------
def process_ticker_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    处理和标准化 Tiingo 股票列表数据
    
    Args:
        df: 原始 DataFrame
        
    Returns:
        标准化后的 DataFrame
        
    处理步骤：
        1. 标准化列名
        2. 添加下载时间戳
        3. 过滤无效数据
        4. 排序
    """
    # 复制数据，避免修改原始 DataFrame
    df = df.copy()
    
    # 添加检索时间
    df['retrieved_at'] = date.today()
    
    # 移除可能的空值
    before = len(df)
    df = df.dropna(subset=['ticker'])
    after = len(df)
    
    if before > after:
        logger.info(f"🧹 移除 {before - after} 条空值记录")
    
    # 按股票代码排序
    df = df.sort_values('ticker')
    
    logger.info(f"✅ 数据处理完成，最终 {len(df)} 条有效记录")
    
    return df


# ----------------------------------------------------------------------------------------------------------------------------------------
# 保存股票列表到 CSV
# ----------------------------------------------------------------------------------------------------------------------------------------
def save_all_listed_stocks_csv() -> bool:
    """
    下载 Tiingo 股票列表并保存为 CSV
    
    Returns:
        成功返回 True，失败返回 False
    """
    # 下载股票列表
    df = download_tiingo_supported_tickers()
    
    if df is None:
        logger.error("❌ 无法下载股票列表")
        return False
    
    # 处理数据
    df = process_ticker_data(df)
    
    # 保存到 CSV
    csv_dir = get_config_value("path.csv_dir")
    output_path = os.path.join(csv_dir, "all_us_stocks_listed.csv")
    
    try:
        df.to_csv(output_path, index=False)
        logger.info(f"✅ 股票列表已保存: {output_path}")
        logger.info(f"📊 统计: 共 {len(df)} 支股票")
        
        # 显示数据概览
        if 'exchange' in df.columns:
            exchange_counts = df['exchange'].value_counts()
            logger.info(f"📍 交易所分布:\n{exchange_counts}")
        
        if 'assetType' in df.columns:
            asset_counts = df['assetType'].value_counts()
            logger.info(f"📈 资产类型分布:\n{asset_counts}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 保存文件失败: {e}")
        return False


# ----------------------------------------------------------------------------------------------------------------------------------------
# 主程序入口
# ----------------------------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("开始下载 Tiingo 美股股票列表")
    logger.info("=" * 60)
    
    success = save_all_listed_stocks_csv()
    
    if success:
        logger.info("=" * 60)
        logger.info("✅ 美股股票列表下载完成")
        logger.info("=" * 60)
    else:
        logger.error("=" * 60)
        logger.error("❌ 下载失败，请查看日志")
        logger.error("=" * 60)
