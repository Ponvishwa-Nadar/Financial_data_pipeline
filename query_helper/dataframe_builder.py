import pandas as pd
def build(l):
    df = pd.DataFrame(l, columns=['timestamp', 'timeframe', 'symbol', 'token', 'open_price', 'high_price', 'low_price', 'close_price', 'volume'])
    df.set_index('timestamp', inplace=True)
    df.index = pd.to_datetime(df.index)
    df.sort_index(axis=0, inplace=True)
    if df.index.tz is None:
        df.index = df.index.tz_localize("Asia/Kolkata")
    df.index = df.index.tz_convert("Asia/Kolkata")
    return df   
