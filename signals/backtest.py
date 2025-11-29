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

def load_price_and_signals() -> pd.DataFrame:
    """
    Load daily prices and signals into a sngle DataFrame.
    Columns:
    - ticker
    - trade_date
    - price
    - momentum_12_1
    """
    conn = get_connection()
    try:
        query = """
        SELECT 
            p.ticker,
            p.trade_date,
            p.adj_close AS price,
            s.momentum_12_1
        FROM prices_daily p
        JOIN daily_signals s
        ON p.ticker = s.ticker AND p.trade_date = s.trade_date
        ORDER BY p.ticker, p.trade_date;
        """
        df = pd.read_sql(query, conn, parse_dates=["trade_date"])
    finally:
        conn.close()
    return df

def compute_daily_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a DataFrame with columns:
    - ticker
    - trade_date
    - price
    -momentum_12_1
    Compute daily returns and add as a new column 'daily_return'.
    Compute daily returns for a "top10_momentum_daily" strategy."""

    df = df.sort_values(["ticker", "trade_date"]).copy()
    df["daily_return"] = df.groupby("ticker")["price"].pct_change()

    # Strategy: Top 10 momentum stocks each day
    df_valid = df.dropna(subset=["momentum_12_1", "daily_return"]).copy()
    def top10_equal_weight(group: pd.DataFrame) -> float | None:
        """
        For a given date group, pick top 10 momentum stocks and return the equal-weight
        average of their daily returns."""

        top = group.sort_values("momentum_12_1", ascending=False).head(10)
        if top.empty:
            return None
        return top["daily_return"].mean()
    
    daily_return_series = df_valid.groupby("trade_date", group_keys=False).apply(top10_equal_weight)
    daily_return= daily_return_series.dropna().reset_index(name="daily_return")

    daily_return = daily_return.sort_values("trade_date").copy()
    daily_return["strategy_name"] = "top10_momentum_daily"
    daily_return["portfolio_value"] = (1+ daily_return["daily_return"]).cumprod()
    return daily_return[["strategy_name", "trade_date", "daily_return", "portfolio_value"]]

def insert_backtest_results(df_bt: pd.DataFrame) -> None:
    """
    Insert backtest results into the backtest_equity table.
    """

    rows = [
        (
            row["strategy_name"],
            row["trade_date"],
            row["portfolio_value"],
            row["daily_return"],
        )
        for _, row in df_bt.iterrows()
    ]
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                sql = """
                INSERT INTO backtest_equity 
                (strategy_name, trade_date, portfolio_value, daily_return)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (strategy_name, trade_date) DO UPDATE SET
                portfolio_value = EXCLUDED.portfolio_value,
                daily_return = EXCLUDED.daily_return;
                """
                execute_batch(cur, sql, rows, page_size=1000)
        print("Backtest results inserted successfully.")
    finally:
        conn.close()

def main():
    df = load_price_and_signals()
    print(f"Loaded {len(df)} price and signal records.")

    df_bt = compute_daily_returns(df)
    print(f"Computed {len(df_bt)} backtest daily return records.")

    insert_backtest_results(df_bt)
if __name__ == "__main__":
    main()
