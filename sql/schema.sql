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

CREATE TABLE IF NOT EXISTS backtest_equity(
    id SERIAL PRIMARY KEY,
    strategy_name TEXT NOT NULL,
    trade_date DATE NOT NULL,
    portfolio_value NUMERIC(18, 6),
    daily_return NUMERIC(18, 6),
    CONSTRAINT unique_backtest_equity UNIQUE(strategy_name, trade_date)

);

CREATE OR REPLACE VIEW v_latest_signals AS 
WITH latest_dates AS (
    SELECT ticker, MAX(trade_date) AS latest_dates
    FROM daily_signals
    GROUP BY ticker
)
SELECT
    s.ticker,
    s.trade_date,
    p.adj_close AS price,
    s.momentum_12_1,
    s.sma_20,
    s.sma_50,
    s.sma_200,
    s.sma_stack_flag,
    s.rsi_14,
    s.rsi_band
FROM daily_signals s
JOIN latest_dates ld
    ON s.ticker = ld.ticker AND s.trade_date = ld.latest_dates
JOIN prices_daily p
    ON s.ticker = p.ticker AND s.trade_date = p.trade_date
ORDER BY s.ticker;

CREATE OR REPLACE VIEW v_signal_history AS
SELECT
    s.ticker,
    s.trade_date,
    p.adj_close AS price,
    s.momentum_12_1,
    s.sma_20,
    s.sma_50,
    s.sma_200,
    s.sma_stack_flag,
    s.rsi_14,
    s.rsi_band
FROM daily_signals s
JOIN prices_daily p
    ON s.ticker = p.ticker AND s.trade_date = p.trade_date
ORDER BY s.ticker, s.trade_date;

CREATE OR REPLACE VIEW v_backtest_equity AS
SELECT
    strategy_name,
    trade_date,
    portfolio_value,
    daily_return
FROM backtest_equity
ORDER BY strategy_name, trade_date;