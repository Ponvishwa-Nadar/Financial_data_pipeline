import pandas as pd
import os
import psycopg2
from datetime import datetime, timedelta
import connection
import dataframe_builder
from loguru import logger
tf_map = {'1m': '1 minute', '5m': '5 minutes', '10m': '10 minutes', '15m': '15 minutes', '1h': 'hour', '1d': 'day', '1M': 'month'}
intra_day_tf = {'1m', '5m', '10m', '15m'}
def fetch_candles(symbol: str, timeframe: str, start_date: str, end_date: str):
    conn = connection.connection()
    if conn is None:
        raise ConnectionError("database connection failed")
    if not symbol:
         raise ValueError("symbol cannot be empty please use get_all_symbols() to see all the available symbols")
    
    cur = conn.cursor()

    tf_value = tf_map.get(timeframe)
    d1 = datetime.strptime(start_date, '%Y-%m-%d')
    d2 = datetime.strptime(end_date, '%Y-%m-%d')
    end_date = datetime.strftime((d2 + timedelta(days=1)), '%Y-%m-%d')
    if d1 > d2:
        raise ValueError("starting date cannot be greater than ending date ")
    if tf_value is None:
        raise ValueError(f"Invalid timeframe: {timeframe} please enter a valid timeframe from {tf_map.keys()} ")
    try:
        if timeframe in intra_day_tf:
                query = """SELECT date_bin(%s::interval , timestamp , %s::timestamp) AS ts,
                        %s as timeframe,
                        symbol,
                        token,
                        (ARRAY_AGG(open_price ORDER BY timestamp))[1] as open_price,
                        MAX(high_price) as high_price,
                        MIN(low_price) as low_price,
                        (ARRAY_AGG(close_price ORDER BY timestamp DESC))[1] as close_price,
                        SUM(volume) as volume
                        FROM equity_market_candles
                        WHERE symbol=%s
                        AND (timestamp >= %s AND timestamp <= %s)
                        GROUP BY token, symbol, ts
                        ORDER BY token, ts"""

                cur.execute(query, 
                            (tf_map[timeframe], f'{start_date} 09:15', timeframe, symbol, start_date, end_date)        
                            )
        else:
                query = """SELECT date_trunc(%s, timestamp) AS ts,
                    %s as timeframe,
                    symbol,
                    token,
                    (ARRAY_AGG(open_price ORDER BY timestamp))[1] as open_price,
                    MAX(high_price) as high_price,
                    MIN(low_price) as low_price,
                    (ARRAY_AGG(close_price ORDER BY timestamp DESC))[1] as close_price,
                    SUM(volume) as volume
                    FROM equity_market_candles
                    WHERE symbol=%s
                    AND (timestamp >= %s AND timestamp <= %s)
                    GROUP BY token, symbol, ts
                    ORDER BY token, ts"""
                cur.execute(query, 
                                (tf_map[timeframe],timeframe, symbol, start_date, end_date)
                                )

        l = cur.fetchall()
        df = dataframe_builder.build(l=l)
        return df

    except Exception as e:
            logger.warning(f"An error occured due to {e} returned an empty dataframe please retry")
            df = dataframe_builder.build(l=[])
            return df
            
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def get_all_symbols():
     conn = connection.connection()
     if conn is None:
        raise ConnectionError("database connection failed")
     cur = conn.cursor()
     try:
          query = """SELECT DISTINCT symbol FROM equity_market_candles"""
          cur.execute(query)
          rows = cur.fetchall()
          df = pd.DataFrame(rows, columns=['symbol'])
          return df
     except Exception as e:
          logger.warning("an error occured")
          return None
     finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
def fetch_latest(symbol: str, timeframe: str, n: int = 100):
    end = datetime.today().strftime('%Y-%m-%d')
    lookback = 7 if timeframe in {'1m', '5m', '10m', '15m'} else 365
    start = (datetime.today() - timedelta(days=lookback)).strftime('%Y-%m-%d')
    df = fetch_candles(symbol, timeframe, start, end)
    return df.tail(n) if df is not None and not df.empty else df
def fetch_multi(symbols: list, timeframe: str, start_date: str, end_date: str) -> dict:
    return {sym: fetch_candles(sym, timeframe, start_date, end_date) for sym in symbols}