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
from datetime import date
from data_download.input.build_trading_calendar import build_trading_calendar


def annual_update():
    # 每年更新交易日历（未来三年）
    build_trading_calendar(start_date=date(2004, 12, 30), horizon_days=365 * 3,)