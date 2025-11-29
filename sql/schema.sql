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


);

CREATE TABLE IF NOT EXISTS daily_signals(
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    trade_date DATE NOT NULL,
    momentum_12_1 NUMERIC(18,6),
    sma_20 NUMERIC (18,6),
    sma_50 NUMERIC (18,6),
    sma_200 NUMERIC (18,6),
    sma_stack_flag BOOLEAN,
    rsi_14 NUMERIC (18,6),
    rsi_band TEXT,
    UNIQUE(ticker, trade_date)
);