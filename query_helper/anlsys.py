import json
import helper


def summarize_price_action(
    symbol: str,
    timeframe: str,
    start_date: str,
    end_date: str,
) -> str:
    """
    Returns a high-level summary of price action for a stock over a date range.
    Use this instead of raw candle data when you need an overview of how a stock
    has behaved — price range, average volume, overall direction, and volatility.

    Args:
        symbol: NSE stock symbol (e.g. 'RELIANCE', 'TCS')
        timeframe: Candle timeframe. Options: '1m', '5m', '10m', '15m', '1h', '1d', '1M'
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        JSON object with price summary including open, close, high, low,
        average volume, price change percent, and number of candles.
    """
    df = helper.fetch_candles(symbol, timeframe, start_date, end_date)

    if df is None or df.empty:
        return json.dumps({"error": "No data found for the given parameters."})

    first_open = float(df["open_price"].iloc[0])
    last_close = float(df["close_price"].iloc[-1])
    price_change = last_close - first_open
    price_change_pct = (price_change / first_open) * 100

    summary = {
        "symbol": symbol,
        "timeframe": timeframe,
        "start_date": start_date,
        "end_date": end_date,
        "candles": len(df),
        "first_open": round(first_open, 2),
        "last_close": round(last_close, 2),
        "period_high": round(float(df["high_price"].max()), 2),
        "period_low": round(float(df["low_price"].min()), 2),
        "price_change": round(price_change, 2),
        "price_change_percent": round(price_change_pct, 2),
        "average_volume": round(float(df["volume"].astype(float).mean()), 0),
        "total_volume": int(df["volume"].astype(float).sum()),
        "direction": "up" if price_change > 0 else "down" if price_change < 0 else "flat",
    }

    return json.dumps(summary, indent=2)


def detect_trend(
    symbol: str,
    timeframe: str,
    start_date: str,
    end_date: str,
    short_window: int = 20,
    long_window: int = 50,
) -> str:
    """
    Detects the price trend of a stock using two Simple Moving Averages (SMA).
    Compares a short-term SMA against a long-term SMA to determine trend direction.
    Use this when you need to know whether a stock is in an uptrend, downtrend,
    or moving sideways.

    Args:
        symbol: NSE stock symbol (e.g. 'RELIANCE', 'TCS')
        timeframe: Candle timeframe. Options: '1m', '5m', '10m', '15m', '1h', '1d', '1M'
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        short_window: Period for short-term SMA (default 20)
        long_window: Period for long-term SMA (default 50)

    Returns:
        JSON object with trend direction, current SMAs, and latest close price.
    """
    df = helper.fetch_candles(symbol, timeframe, start_date, end_date)

    if df is None or df.empty:
        return json.dumps({"error": "No data found for the given parameters."})

    if len(df) < long_window:
        return json.dumps({
            "error": f"Not enough data to compute {long_window}-period SMA. "
                     f"Got {len(df)} candles, need at least {long_window}."
        })

    closes = df["close_price"].astype(float)
    short_sma = round(float(closes.rolling(window=short_window).mean().iloc[-1]), 2)
    long_sma = round(float(closes.rolling(window=long_window).mean().iloc[-1]), 2)
    latest_close = round(float(closes.iloc[-1]), 2)

    if short_sma > long_sma:
        trend = "uptrend"
        explanation = f"Short SMA ({short_window}) is above Long SMA ({long_window}), indicating bullish momentum."
    elif short_sma < long_sma:
        trend = "downtrend"
        explanation = f"Short SMA ({short_window}) is below Long SMA ({long_window}), indicating bearish momentum."
    else:
        trend = "sideways"
        explanation = "Short and Long SMAs are equal — no clear trend direction."

    return json.dumps({
        "symbol": symbol,
        "trend": trend,
        "explanation": explanation,
        "latest_close": latest_close,
        f"sma_{short_window}": short_sma,
        f"sma_{long_window}": long_sma,
        "short_window": short_window,
        "long_window": long_window,
    }, indent=2)


def analyze_volume(
    symbol: str,
    timeframe: str,
    start_date: str,
    end_date: str,
) -> str:
    """
    Analyzes volume behavior for a stock over a date range.
    Identifies the highest and lowest volume sessions, checks whether recent
    volume is above or below average, and gives an overall volume trend.
    Use this when you need to understand whether price moves are backed by
    strong or weak volume.

    Args:
        symbol: NSE stock symbol (e.g. 'RELIANCE', 'TCS')
        timeframe: Candle timeframe. Options: '1m', '5m', '10m', '15m', '1h', '1d', '1M'
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        JSON object with average volume, highest/lowest volume sessions,
        recent volume vs average, and volume trend assessment.
    """
    df = helper.fetch_candles(symbol, timeframe, start_date, end_date)

    if df is None or df.empty:
        return json.dumps({"error": "No data found for the given parameters."})

    df["volume"] = df["volume"].astype(float)
    avg_volume = df["volume"].mean()

    # Split into first half and second half to detect volume trend
    mid = len(df) // 2
    first_half_avg = df["volume"].iloc[:mid].mean()
    second_half_avg = df["volume"].iloc[mid:].mean()

    if second_half_avg > first_half_avg * 1.1:
        volume_trend = "increasing"
    elif second_half_avg < first_half_avg * 0.9:
        volume_trend = "decreasing"
    else:
        volume_trend = "stable"

    # Highest and lowest volume candle timestamps
    max_vol_idx = df["volume"].idxmax()
    min_vol_idx = df["volume"].idxmin()
    latest_volume = float(df["volume"].iloc[-1])

    return json.dumps({
        "symbol": symbol,
        "total_candles": len(df),
        "average_volume": round(avg_volume, 0),
        "total_volume": int(df["volume"].sum()),
        "highest_volume_session": {
            "timestamp": str(max_vol_idx),
            "volume": int(df["volume"].max()),
        },
        "lowest_volume_session": {
            "timestamp": str(min_vol_idx),
            "volume": int(df["volume"].min()),
        },
        "latest_volume": int(latest_volume),
        "latest_vs_average": "above average" if latest_volume > avg_volume else "below average",
        "volume_trend": volume_trend,
    }, indent=2)