import pandas as pd
from datetime import date
from log_config import logger
from pathlib import Path
class MarketCalendar:
   
    def __init__(self, holiday_csv_path: str):
        if not Path(holiday_csv_path).exists():
            raise FileNotFoundError(f"Holiday CSV not found: {holiday_csv_path}")
        self.holidays = pd.read_csv(
            holiday_csv_path,
            parse_dates=["date"]
        )
        self.holiday_dates = set(self.holidays["date"].dt.date)
    def is_weekend(self, day: date) -> bool:
        # Monday = 0, Sunday = 6
        return day.weekday() >= 5

    def is_holiday(self, day: date) -> bool:
        return day in self.holiday_dates

    def is_market_open(self, day: date = None):
        if day is None:
            day = date.today()

        if self.is_weekend(day):
            return False, "Weekend"

        if self.is_holiday(day):
            reason = self.holidays[
                self.holidays["date"].dt.date == day
            ]["description"].values[0]
            return False, f"NSE Holiday: {reason}"

        return True, "Market Open"
