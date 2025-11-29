# Stock Signal Screener & Backtest Dashboard

Capstone project for the **Google Data Analytics Certificate**.  
Built as a case study in data collection, transformation, analysis, and visualization.

## About this Project

This project was developed as my capstone for the **Google Data Analytics Certificate**.
The goal was to go beyond a basic dashboard and build a realistic analytics pipeline:
Python ETL → PostgreSQL data model → quantitative signals and backtesting → Tableau dashboards.

## Skills Demonstrated
- Data collection and cleaning with Python (pandas, yfinance)
- Relational data modeling in PostgreSQL (prices, signals, backtest results)
- Feature engineering (12–1 momentum, SMA stack, RSI bands)
- Backtesting and performance evaluation
- Dashboard design in Tableau 


## Pipeline Overview

1. **Tickers & Prices**
   - Hard-coded universe of 50 large-cap U.S. stocks (`etl/fetch_prices.py`).
   - Daily OHLCV prices loaded from Yahoo Finance into PostgreSQL (`prices_daily` table).

2. **Signals**
   - 12–1 momentum (12-month minus 1-month window).
   - 20/50/200-day simple moving averages and bullish SMA stack flag.
   - 14-day RSI with categorical bands (oversold/neutral/overbought).
   - Stored in `daily_signals` table.

3. **Backtest**
   - Strategy: `top10_mom_daily`
     - Each day, rank all tickers by 12–1 momentum.
     - Invest equal weight in the top 10 names with valid signals.
     - Track daily portfolio returns and cumulative portfolio value.
   - Results stored in `backtest_equity` table.

4. **SQL Views (Later converted to CSV exports for Tableau Public)**
   - `v_latest_signals`: latest price and signals per ticker (for screener).
   - `v_signal_history`: full price + signal time series.
   - `v_backtest_equity`: strategy equity curve.

5. **Dashboards (Tableau)**
   - **Stock Signal Screener**: latest signals table + linked price+SMA chart.
   - **Backtest Performance**: equity curve and summary performance metrics.

   ## Stock Signal Screener
![Stock Signal Screener](docs/stock_signal_screener.png)

## Backtest Performance
![Backtest Performance](docs/backtest_performance.png)