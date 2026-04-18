from SmartApi import SmartConnect
from log_config import logger
import authentication
from datetime import datetime, timedelta
import pandas as pd
import time
import database_conn
def fetching(smart, instruments):
    if smart is None:
        logger.error("could not establish a connection to api")
        raise RuntimeError("Smart API connection not established")
    conn = database_conn.get_db_connection()
    try:
        last_date = database_conn.get_last_date(conn)
    finally:
        conn.close()
    start_time = (last_date + timedelta(days=1)).strftime("%Y-%m-%d 09:15")
    end_time = datetime.now().strftime("%Y-%m-%d 15:30")

    if len(instruments) == 0:
        logger.info("instruments list is empty")
        return pd.DataFrame(columns=['timestamp', 'symbol', 'token', 'open','high','low','close', 'volume'])

    all_dfs = []        

    for i in instruments:

        try:

            candles = smart.getCandleData({
                "exchange": i['exchange'],
                "symboltoken": i['token'],        
                "interval": "ONE_MINUTE",    
                "fromdate": start_time,
                "todate": end_time
                })
            
            if candles.get("status") and candles.get("data"):

                df = pd.DataFrame(candles['data'], columns=['timestamp','open','high','low','close', 'volume'])
                df['symbol'] = i['symbol']
                df['token'] = i['token']
                df = df[['timestamp', 'symbol', 'token', 'open','high','low','close', 'volume']]
                all_dfs.append(df)
                logger.info(f"{len(df)} amount of rows fetch for {i['symbol']}")
            
            else:
                logger.warning("Could not get candles")
                continue
    
        except Exception as e:
            logger.exception(f"Could not get candles because: {e}")
        time.sleep(0.5)

    if not all_dfs:
        logger.warning("No candle data fetched for any instrument")
        return pd.DataFrame(
        columns=['timestamp','symbol','token','open','high','low','close','volume']
        )
    logger.info(f"total number of rows fetched is {len(pd.concat(all_dfs, ignore_index=True))}")
    return pd.concat(all_dfs, ignore_index=True)

    
