from ui.data_sources.portfolio_nav import PortfolioNAVSource
from ui.data_sources.ticker_nav import TickerNAVSource
from ui.transforms.align import align_series
from ui.plots.nav_plot import plot_nav


def compare_portfolio_with_tickers(
    tickers: list[str],
    start_date: str = None,
    end_date: str = None,
):
    sources = {"Portfolio": PortfolioNAVSource(start_date, end_date)}

    for t in tickers:
        sources[t] = TickerNAVSource(t, start_date, end_date)

    data = {name: src.load() for name, src in sources.items()}

    aligned = align_series(list(data.values()))
    data = dict(zip(data.keys(), aligned))

    plot_nav(data)
