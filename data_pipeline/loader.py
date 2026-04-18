import pandas as pd
from log_config import logger
from pathlib import Path
BASE_DIR = Path(__file__).parent

def instru_loader(csv_path=BASE_DIR / "master_key.csv"):
    try:
        df = pd.read_csv(csv_path)
        if df.empty:
            logger.error("instruments is empty because master file is empty; returning empty list")
            return []
        
        df['symbol'] = df['symbol'].str.replace("-EQ", "", regex=False)
        instruments = [
        {
        "symbol": row.symbol,
        "token": str(row.token),
        "exchange": "NSE"
        }
        for row in df.itertuples(index=False)
            ]
        logger.info("instruments loaded successfully")
        return instruments
        
    except Exception as e:
        logger.exception(f"failed to retrive data from master file due to : {e}")
        return []
