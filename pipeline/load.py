from sqlalchemy import create_engine, text
import pandas as pd
from config import DB_URL
import traceback

# Load function to write DataFrame to the database
def load(df: pd.DataFrame, table_name: str):
    try:
        engine = create_engine(DB_URL)

        create_table = f""" CREATE TABLE IF NOT EXISTS {table_name} (
            org_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            credit_type TEXT,
            action TEXT,
            credits DOUBLE PRECISION,
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
            conn.execute(text(create_table))
            conn.commit()
        
        df.to_sql(table_name, engine, if_exists='append', index=False)

    except Exception:
        print(traceback.format_exc())
        raise