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

from data_download.input.price_downloader import download_single_instrument_prices
from tasks.backtest_tasks import run_backtest
from tasks.daily_tasks import daily_update
from ui.api import compare_portfolio_with_tickers
from reports.daily_briefing import run_briefing

if __name__ == "__main__":
    #annual_update()
    #seasonal_update()
    #daily_update()
    
    run_briefing()

    # run_backtest()

    #compare_portfolio_with_tickers(
    #    tickers=["MSFT", "AAPL", "SPY", "LITE", "AMZN", "NVDA", "AMD", "TSLA"],
    #    start_date="2019-01-01",
    #)
    
    #download_single_instrument_prices("SPCX", date(2026, 6, 1), date(2026, 6, 14))
    
    pass

