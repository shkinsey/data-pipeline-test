
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
import os

def test_db_connection(db_url):

    engine = create_engine(db_url)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Database connection successful.")
    except OperationalError as e:
        print("❌ Database connection failed.")
        raise e
