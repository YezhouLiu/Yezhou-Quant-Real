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
from data_download.input.corporate_actions_extractor import extract_corporate_actions
from data_download.input.fundamentals_downloader import download_fundamentals
from data_download.input.price_downloader import download_prices
from data_download.update.update_tradable_universe import update_tradable_universe
from engine.compute_factors.compute_all_factors import compute_all_factors


def daily_update():
    # 最近股价下载
    download_prices()
    
    # 公司行为抽取 我暂时没开始处理fundamentals的raw数据 先备注
    # extract_corporate_actions()
    compute_all_factors()
    
    # 每日更新可交易标的
    update_tradable_universe()
    