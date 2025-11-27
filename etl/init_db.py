import psycopg2
from psycopg2.extras import execute_batch
from pathlib import Path
from etl.config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

def get_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

def run_schema():
    """
    Read the schema.sql file and execute it against the PostgreSQL database.
    """
    schema_path = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(schema_sql)
        print("Schema applied successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    run_schema()
