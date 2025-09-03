from sqlalchemy import create_engine, text
import pandas as pd
from config import DB_URL, TABLE_NAME


def load(df: pd.DataFrame) -> pd.DataFrame:
    engine = create_engine(DB_URL)

    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_NAME}"))
        conn.commit()

    df.to_sql(TABLE_NAME, engine, if_exists='append', index=False)

    return pd.read_sql(f"SELECT * FROM {TABLE_NAME}", engine)
