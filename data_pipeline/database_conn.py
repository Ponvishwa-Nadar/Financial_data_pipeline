import os
import psycopg2
from psycopg2.extras import execute_batch
from log_config import logger
from datetime import datetime, timedelta
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("HOST"),
            port=os.getenv("PORT"),
            dbname=os.getenv("DBNAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        conn.autocommit = False
        logger.info("Database connection established")
        return conn

    except Exception as e:
        logger.exception(f"Failed to connect to database: {e}")
        raise

def get_last_date(conn):
    
    q = """SELECT MAX(DATE(timestamp)) FROM equity_market_candles"""
    with conn.cursor() as cur:
        cur.execute(q)
        last_date = cur.fetchone()[0]
    
    if last_date is None:
        logger.warning("Table is empty, defaulting to yesterday's date")
        return datetime.now().date() - timedelta(days=1)
    return last_date
    

def write_equity_candles(conn, df):
    """
    Insert candle data into PostgreSQL with UPSERT
    """
    if df.empty:
        logger.warning("No data to insert into database")
        return

    insert_sql = """
        INSERT INTO equity_market_candles (
            timestamp,
            token,
            symbol,
            open_price,
            high_price,
            low_price,
            close_price,
            volume
        )
        VALUES (
            %(timestamp)s,
            %(token)s,
            %(symbol)s,
            %(open_price)s,
            %(high_price)s,
            %(low_price)s,
            %(close_price)s,
            %(volume)s
        )
        ON CONFLICT (timestamp, token)
        DO UPDATE SET
            open_price  = EXCLUDED.open_price,
            high_price  = EXCLUDED.high_price,
            low_price   = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume      = EXCLUDED.volume;
    """

    records = df.rename(columns={
        "open": "open_price",
        "high": "high_price",
        "low": "low_price",
        "close": "close_price"
    }).to_dict("records")

    try:
        with conn.cursor() as cur:
            execute_batch(cur, insert_sql, records, page_size=1000)
        conn.commit()
        logger.info(f"Inserted/updated {len(records)} rows")

    except Exception as e:
        conn.rollback()
        logger.exception(f"Failed to insert data: {e}")
        raise
