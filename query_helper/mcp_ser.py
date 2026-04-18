import json
import helper
import anlsys
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("nse-data-server")


def df_to_json(df) -> str:
    """Convert a DataFrame to a JSON string. Resets index so timestamp is included."""
    if df is None or df.empty:
        return json.dumps([])
    df_reset = df.reset_index()
    # Convert timestamps to string so they are JSON serializable
    for col in df_reset.select_dtypes(include=["datetimetz", "datetime64[ns]"]).columns:
        df_reset[col] = df_reset[col].astype(str)
    return df_reset.to_json(orient="records", indent=2)


@mcp.tool()
def get_all_symbols() -> str:
    """
    Returns a list of all stock symbols available in the database.
    Always call this first if the user hasn't specified a symbol,
    or if you need to verify a symbol exists before querying candle data.
    """
    df = helper.get_all_symbols()
    if df is None or df.empty:
        return json.dumps([])
    return json.dumps(df["symbol"].tolist())


@mcp.tool()
def fetch_candles(
    symbol: str,
    timeframe: str,
    start_date: str,
    end_date: str,
) -> str:
    """
    Fetch OHLCV (Open, High, Low, Close, Volume) candle data for a stock.

    Args:
        symbol: NSE stock symbol (e.g. 'RELIANCE', 'TCS', 'INFY').
                Use get_all_symbols() to see what's available.
        timeframe: Candle timeframe. Options: '1m', '5m', '10m', '15m', '1h', '1d', '1M'
        start_date: Start date in YYYY-MM-DD format (e.g. '2024-01-01')
        end_date: End date in YYYY-MM-DD format (e.g. '2024-03-31')

    Returns:
        JSON array of candles, each with timestamp, open, high, low, close, volume.
    """
    df = helper.fetch_candles(symbol, timeframe, start_date, end_date)
    return df_to_json(df)


@mcp.tool()
def fetch_latest(
    symbol: str,
    timeframe: str,
    n: int = 100,
) -> str:
    """
    Fetch the most recent N candles for a stock symbol.
    Use this when you need the latest price data without specifying a date range.

    Args:
        symbol: NSE stock symbol (e.g. 'RELIANCE', 'TCS', 'INFY').
                Use get_all_symbols() to see what's available.
        timeframe: Candle timeframe. Options: '1m', '5m', '10m', '15m', '1h', '1d', '1M'
        n: Number of most recent candles to return (default 100)

    Returns:
        JSON array of the last N candles with timestamp, open, high, low, close, volume.
    """
    df = helper.fetch_latest(symbol, timeframe, n)
    return df_to_json(df)


@mcp.tool()
def fetch_multi(
    symbols: list[str],
    timeframe: str,
    start_date: str,
    end_date: str,
) -> str:
    """
    Fetch OHLCV candle data for multiple stock symbols at once.
    Use this when you need to compare or analyze several stocks over the same period.

    Args:
        symbols: List of NSE stock symbols (e.g. ['RELIANCE', 'TCS', 'INFY'])
        timeframe: Candle timeframe. Options: '1m', '5m', '10m', '15m', '1h', '1d', '1M'
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        JSON object where each key is a symbol and value is its array of candles.
    """
    result = helper.fetch_multi(symbols, timeframe, start_date, end_date)
    serialized = {}
    for symbol, df in result.items():
        serialized[symbol] = json.loads(df_to_json(df))
    return json.dumps(serialized, indent=2)
mcp.add_tool(anlsys.summarize_price_action)
mcp.add_tool(anlsys.detect_trend)
mcp.add_tool(anlsys.analyze_volume)

if __name__ == "__main__":
    mcp.run()