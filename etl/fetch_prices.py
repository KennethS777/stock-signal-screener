import datetime as dt
from typing import List
import yfinance as yf
import pandas as pd


def get_ticker_list()-> List[str]:
    """
    Retrieve a list of stock tickers to fetch prices for.
    For demonstration, we return a static list. In a real scenario,"""
    return ["AAPL", "MSFT", "GOOGL", "AMZN", "META", 
            "JPM", "JNJ", "V", "PG", "XOM",]

def fetch_daily_prices(tickers: List[str],
                       start_date: str = "2015-01-01",
                       end_date: str = None) -> pd.DataFrame:
    """
    Fetch daily stock prices for thegivenlist of tickers from Yahoo Finance."""

    if end_date is None:
        end_date = dt.datetime.now().strftime("%Y-%m-%d")
    all_data = []
    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        data = yf.download(ticker, start=start_date, end=end_date, auto_adjust=False)
        if data.empty:
            print(f"No data found for {ticker}. Skipping.")
            continue
        data.reset_index(inplace=True)

        # yfinance.download sometimes returns MultiIndex columns when
        # the second level is the ticker symbol (e.g. ('Close', 'AAPL')).
        # That makes attribute access on itertuples() produce generic names
        # (_0, _1, ...) instead of expected field names like `volume`.
        # Normalize columns to single-level strings by picking the first
        # non-empty label in a tuple if needed.
        if isinstance(data.columns, pd.MultiIndex):
            flat_cols = []
            for col in data.columns:
                # `col` can be a tuple like ('Adj Close', 'AAPL') or ('Date','')
                if isinstance(col, tuple):
                    # choose the first non-empty piece; fall back to "" -> join
                    chosen = next((c for c in col if str(c).strip() != ""), "")
                    flat_cols.append(chosen)
                else:
                    flat_cols.append(col)
            data.columns = flat_cols

        data = data.rename(columns={
            "Date": "trade_date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume"
        })

        data["ticker"] = ticker
        all_data.append(data)

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        raise RuntimeError("No data fetched for any ticker.")

if __name__ == "__main__":
    df = fetch_daily_prices(get_ticker_list())
    print(df.head())
    
