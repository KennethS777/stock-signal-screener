CREATE TABLE IF NOT EXISTS prices_daily(
    id SERIAL PRIMARY KEY, 
    ticker TEXT NOT NULL,
    trade_date DATE NOT NULL,
    open NUMERIC(18, 6),
    high NUMERIC(18, 6),
    low NUMERIC(18, 6),
    close NUMERIC(18, 6),
    adj_close NUMERIC(18, 6),
    volume BIGINT,
    UNIQUE(ticker, trade_date)
    

)