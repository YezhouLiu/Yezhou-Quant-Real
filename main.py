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
from tasks.backtest_tasks import run_backtest
from tasks.daily_tasks import daily_update
from ui.api import compare_portfolio_with_tickers

if __name__ == "__main__":
    daily_update()
    # seasonal_update()
    # annual_update()

    # run_backtest()

    compare_portfolio_with_tickers(
        tickers=["MSFT", "AAPL", "SPY", "LITE", "AMZN", "NVDA", "AMD", "TSLA"],
        start_date="2019-01-01",
    )
    pass
