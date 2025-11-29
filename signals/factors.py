import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

from etl.config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

def get_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

def load_price_history() -> pd.DataFrame:
    """
    Load price history from the prices_daily table into a DataFrame.
    """
    conn = get_connection()
    try:
        query = """
        SELECT ticker, trade_date, adj_close AS PRICE
        FROM prices_daily
        ORDER BY ticker, trade_date;
        """
        df = pd.read_sql(query, conn, parse_dates=["trade_date"])
    finally:
        conn.close()
    return df


def compute_signals_for_ticker(df_ticker: pd.DataFrame) -> pd.DataFrame:
    """
    Given the dataframe for a single ticker, compute:
    - 20/50/200 day moving averages and bullish stack flag
    - 14-day RSI
    - 12-1 Momentum
    Returns a DataFrame with the computed signals.
    """

    df = df_ticker.sort_values("trade_date").copy()

    # 12-1 Momentum, 252 trading days = 12 months, 21 days = 1 month
    df["momentum_12_1"] = df["price"].shift(21) / df["price"].shift(252) - 1

    df["sma_20"] = df["price"].rolling(window=20, min_periods=20).mean()
    df["sma_50"] = df["price"].rolling(window=50, min_periods=50).mean()
    df["sma_200"] = df["price"].rolling(window=200, min_periods=200).mean()

    df["sma_stack_flag"] = (
        (df["price"] > df["sma_20"]) &
        (df["sma_20"] > df["sma_50"]) &
        (df["sma_50"] > df["sma_200"])
    )

    # 14 day RSI
    delta = df["price"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    window = 14
    avg_gain = gain.rolling(window=window, min_periods=window).mean()
    avg_loss = loss.rolling(window=window, min_periods=window).mean()

    rs = avg_gain / avg_loss
    df["rsi_14"] = 100 - (100 / (1 + rs))


    def classify_rsi(val: float | None) -> str | None:
        """
        Classify RSI value into categories.
        - "oversold" if RSI < 30
        - "overbought" if RSI > 70
        - "neutral" if RSI is between 30 and 70 or NaN
        """

        if pd.isna(val):
            return None
        elif val < 30:
            return "oversold"
        elif val > 70:
            return "overbought"
        else:
            return "neutral"

    df["rsi_band"] = df["rsi_14"].apply(classify_rsi)

    signals = df[["ticker", "trade_date", "momentum_12_1", "sma_20", "sma_50", "sma_200",
                "sma_stack_flag", "rsi_14", "rsi_band"]].copy()
    return signals



def compute_all_signals(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Groups the full price history by ticker and 
    computes signals for each ticker and comibines 
    the results into a single DataFrame."""

    all_signals = []
    for ticker, df_ticker in prices.groupby("ticker"):
        signals = compute_signals_for_ticker(df_ticker)
        all_signals.append(signals)

    return pd.concat(all_signals, ignore_index=True)

def insert_signals(df_signals: pd.DataFrame) -> None:
    """
    Insert or update signals in the daily_signals table."""
    rows = [
        (
        row["ticker"],
        row["trade_date"],      
        row["momentum_12_1"],
        row["sma_20"],
        row["sma_50"],
        row["sma_200"],
        bool(row["sma_stack_flag"]) if pd.notna(row["sma_stack_flag"]) else None,
        row["rsi_14"],
        row["rsi_band"],
    )
    for _, row in df_signals.iterrows()   
    ]

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                sql ="""
                INSERT INTO daily_signals (
                    ticker, trade_date, momentum_12_1,
                    sma_20, sma_50, sma_200,
                    sma_stack_flag, rsi_14, rsi_band
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticker, trade_date) DO UPDATE SET
                    momentum_12_1 = EXCLUDED.momentum_12_1,
                    sma_20 = EXCLUDED.sma_20,
                    sma_50 = EXCLUDED.sma_50,
                    sma_200 = EXCLUDED.sma_200,
                    sma_stack_flag = EXCLUDED.sma_stack_flag,
                    rsi_14 = EXCLUDED.rsi_14,
                    rsi_band = EXCLUDED.rsi_band;
                    """
                execute_batch(cur, sql, rows, page_size=1000)
        print("Signals inserted/updated successfully.")
    finally:
        conn.close()

def main():
    prices = load_price_history()
    print (f"Loaded {len(prices)} price records.")

    signals = compute_all_signals(prices)
    print (f"Computed {len(signals)} signal rows.")

    insert_signals(signals)

if __name__ == "__main__":
    main()

