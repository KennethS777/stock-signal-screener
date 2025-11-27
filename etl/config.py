import os
from dotenv import load_dotenv

load_dotenv()

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "stock_screener")
PG_USER = os.getenv("PG_USER", "stocks_user")
PG_PASSWORD = os.getenv("PG_PASSWORD")
if PG_PASSWORD is None:
    raise ValueError("PG_PASSWORD environment variable is not set.")

