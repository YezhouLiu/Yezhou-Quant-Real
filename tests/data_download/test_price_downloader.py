"""
价格下载器功能测试（不碰真实DB）
================================

测试Tiingo API调用和数据转换逻辑
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import os
os.chdir(project_root)

from datetime import date
import requests

from data_download.input.tiingo_downloader import fetch_tiingo_prices, transform_tiingo_to_db_format
from utils.config_loader import get_config_value


def test_fetch_and_transform():
    """测试API调用和数据转换"""
    
    print("🧪 测试 Tiingo API 调用和数据转换\n")
    
    # 获取API token
    api_token = get_config_value("tiingo.api_key")
    if not api_token:
        print("❌ 未配置 Tiingo API Token")
        return False
    
    print(f"✅ API Token: {api_token[:10]}...\n")
    
    # 测试ticker
    test_ticker = "AAPL"
    test_start = date(2024, 1, 1)
    test_end = date(2024, 1, 5)
    test_instrument_id = 999  # Mock ID
    
    print(f"📊 测试参数:")
    print(f"   Ticker: {test_ticker}")
    print(f"   日期: {test_start} → {test_end}")
    print(f"   Mock Instrument ID: {test_instrument_id}\n")
    
    # 测试fetch
    with requests.Session() as session:
        print("🔗 调用 Tiingo API...")
        tiingo_data = fetch_tiingo_prices(test_ticker, test_start, test_end, api_token, session)
        
        if not tiingo_data:
            print("❌ 未获取到数据")
            return False
        
        print(f"✅ 获取 {len(tiingo_data)} 条记录\n")
        
        # 显示样例
        print("📋 第一条原始数据:")
        first_record = tiingo_data[0]
        for key, value in first_record.items():
            print(f"   {key}: {value}")
        print()
        
        # 测试transform
        print("🔄 转换为数据库格式...")
        db_records = transform_tiingo_to_db_format(tiingo_data, test_instrument_id)
        
        if not db_records:
            print("❌ 转换失败")
            return False
        
        print(f"✅ 转换成功，得到 {len(db_records)} 条记录\n")
        
        # 显示转换后样例
        print("📋 第一条转换后数据:")
        first_db_record = db_records[0]
        for key, value in first_db_record.items():
            print(f"   {key}: {value}")
        print()
        
        # 验证必要字段
        print("🔍 验证必要字段...")
        required_fields = ['instrument_id', 'date', 'close_price', 'adj_close', 'data_source']
        missing_fields = [f for f in required_fields if f not in first_db_record]
        
        if missing_fields:
            print(f"❌ 缺失字段: {missing_fields}")
            return False
        
        print("✅ 所有必要字段存在\n")
        
        # 验证数据类型
        print("🔍 验证数据类型...")
        assert isinstance(first_db_record['instrument_id'], int)
        assert isinstance(first_db_record['date'], str)
        assert first_db_record['data_source'] == 'tiingo'
        print("✅ 数据类型正确\n")
    
    print("=" * 50)
    print("✅ 所有测试通过！")
    print("=" * 50)
    print("\n💡 Tiingo API 和数据转换功能正常")
    print("💡 可以安全运行 price_downloader.py\n")
    
    return True


if __name__ == "__main__":
    success = test_fetch_and_transform()
    sys.exit(0 if success else 1)
