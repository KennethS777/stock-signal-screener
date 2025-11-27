from typing import Iterable
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd

from etl.config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
from etl.fetch_prices import fetch_daily_prices, get_ticker_list

def get_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

def insert_prices(rows:Iterable[tuple])-> None:
    """
    Insert an iterable of row tuples into the prices_daily table."""

    conn = get_connection()
    try: 
        with conn:
            with conn.cursor() as cur:
                sql = """
                INSERT INTO prices_daily 
                (ticker, trade_date, open, high, low, close, adj_close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, trade_date) DO NOTHING;
                """
                execute_batch(cur, sql, rows, page_size =1000)
        print("Data inserted successfully.")
    finally:
        conn.close()

def load_prices_to_db():
    """
    Fetch daily prices and load them into PostgreSQL.
    """

    tickers = get_ticker_list()
    df = fetch_daily_prices(tickers)
    if df.empty:
        print("No data to load into the database.")
        return
    

    rows = []

    for x in df.itertuples(index=False):
        vol = x.volume
        if pd.isna(vol):
            vol = None
        else:
            vol = int(vol)
        
        row = (
            x.ticker,
            x.trade_date,
            x.open,
            x.high,
            x.low,
            x.close,
            x.adj_close,
            vol,
        )
        rows.append(row)

    insert_prices(rows)

if __name__ == "__main__":
    load_prices_to_db()    

