from tasks.backtest_tasks import run_backtest
from ui.api import compare_portfolio_with_tickers

if __name__ == "__main__":
    # daily_update()
    # seasonal_update()
    # annual_update()

    run_backtest()

    compare_portfolio_with_tickers(
        tickers=["MSFT", "AAPL", "SPY"],
        start_date="2019-01-01",
    )
    pass
