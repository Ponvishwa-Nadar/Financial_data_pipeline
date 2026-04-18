import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()
def connection():
    try:
        conn = psycopg2.connect(host=os.getenv("host"),
                            port=os.getenv("port"),
                            dbname=os.getenv("db_name"),
                            user=os.getenv("db_user"),
                            password=os.getenv("DB_PASSWORD"))
        return conn
    except Exception as e:
        return None
