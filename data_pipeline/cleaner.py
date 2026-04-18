import pandas as pd
from log_config import logger

EXPECTED_COLUMNS = [
    "timestamp",
    "symbol",
    "token",
    "open",
    "high",
    "low",
    "close",
    "volume"
]

def clean_equity_candles(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        logger.warning("Cleaner received empty DataFrame")
        return df

    logger.info(f"Starting cleaning on {len(df)} rows")

    # 1. Column validation
    missing_cols = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df = df[EXPECTED_COLUMNS].copy()

    # 2. Timestamp normalization
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata")

    # 3. Numeric coercion
    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 4. Drop invalid rows
    before = len(df)
    df.dropna(
        subset=["timestamp", "open", "high", "low", "close"],
        inplace=True
    )
    dropped = before - len(df)
    if dropped > 0:
        logger.warning(f"Dropped {dropped} invalid rows")

    # 5. Logical OHLC validation
    df = df[
        (df["high"] >= df["low"]) &
        (df["high"] >= df["open"]) &
        (df["high"] >= df["close"]) &
        (df["low"] <= df["open"]) &
        (df["low"] <= df["close"])
    ]

    # 6. Deduplication
    df.drop_duplicates(
        subset=["timestamp", "token"],
        keep="last",
        inplace=True
    )

    # 7. Sorting
    df.sort_values(
        by=["token", "timestamp"],
        inplace=True
    )

    df.reset_index(drop=True, inplace=True)

    logger.info(f"Cleaning complete: {len(df)} rows remaining")

    return df
