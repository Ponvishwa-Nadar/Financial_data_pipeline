import os
import pyotp
from SmartApi import SmartConnect
from log_config import logger
from dotenv import load_dotenv
import time


def authenticator(max_retries=3, waiting_time=30):
    load_dotenv()
    API_KEY = os.getenv("ANGEL_API_KEY")
    CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
    PASSWORD = os.getenv("ANGEL_PASSWORD")  
    TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

    if not all([API_KEY, CLIENT_ID, PASSWORD, TOTP_SECRET]):
        logger.error("Missing Angel One credentials in environment variables")
        return None

    for i in range(1, max_retries+1):
        try:
            logger.info(f"Trying for {i}/{max_retries}")
            smart = SmartConnect(api_key=API_KEY)
            totp = pyotp.TOTP(TOTP_SECRET).now()
            session = smart.generateSession(CLIENT_ID, PASSWORD, totp)

            if session and session.get("status"):
                logger.info(f"API authentication successful on attempt {i}/{max_retries}")
                return smart
            else:
                logger.warning(f"Authentication failed on attempt {i}/{max_retries}")
                time.sleep(waiting_time * i)

        except Exception as e:
            logger.exception(f"Exception on attempt {i}/{max_retries}: {e}")
            time.sleep(waiting_time * i)

    logger.error("All authentication attempts exhausted")
    return None
            
               
                

