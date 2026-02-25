import pandas as pd
from database.readwrite.rw_instruments import get_instrument_id
from database.readwrite.rw_market_prices import get_prices
from database.utils.db_utils import get_db_connection


class TickerNAVSource:
    """
    数据来源：market_prices
    """

    def __init__(self, ticker: str, start_date: str = None, end_date: str = None):
        self.ticker = ticker.upper()
        self.start_date = start_date
        self.end_date = end_date

    def load(self) -> pd.DataFrame:
        with get_db_connection() as conn:
            instrument_id = get_instrument_id(conn, self.ticker)

            if not instrument_id:
                raise RuntimeError(f"Ticker not found: {self.ticker}")

            df = get_prices(
                conn,
                instrument_id=instrument_id,
                start_date=self.start_date,
                end_date=self.end_date,
            )

        if df.empty:
            raise RuntimeError(f"No price data for {self.ticker}")

        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")

        first_price = df["adj_close"].iloc[0]
        df["nav"] = df["adj_close"] / first_price

        return df[["nav"]]
