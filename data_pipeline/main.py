from datetime import datetime
from log_config import logger
import authentication
import loader
import fetcher
import cleaner
import market_calendar
import database_conn
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()
BASE_DIR = Path(__file__).parent

def main():
    logger.info("Equity Market Data Pipeline Started")

    today = datetime.now().date()

    calendar = market_calendar.MarketCalendar(BASE_DIR / "national_holidays.csv")

    is_open, reason = calendar.is_market_open(today)

    if not is_open:
        logger.info(f"Market closed today: {reason}. Exiting.")
        return

    smart = authentication.authenticator()
    if smart is None:
        logger.error("Authentication failed. Exiting pipeline.")
        return

    instruments = loader.instru_loader()
    if not instruments:
        logger.error("No instruments loaded. Exiting pipeline.")
        return

    raw_df = fetcher.fetching(smart, instruments)

    if raw_df.empty:
        logger.warning("Fetcher returned empty dataframe. Exiting.")
        return

    clean_df = cleaner.clean_equity_candles(raw_df)

    if clean_df.empty:
        logger.warning("Cleaned dataframe is empty. Exiting.")
        return

    conn = database_conn.get_db_connection()
    try:
        database_conn.write_equity_candles(conn, clean_df)
    finally:
        conn.close()

    logger.info("Equity Market Data Pipeline Completed Successfully")

if __name__ == "__main__":
    main()
