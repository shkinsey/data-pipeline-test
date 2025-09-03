
import pandas as pd
from sqlalchemy import create_engine, inspect
from config import DB_URL, TABLE_NAME

def check_result():
    engine = create_engine(DB_URL)

    result_df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", engine)
    print(result_df)

    # Print the schema (column names and types)
    inspector = inspect(engine)
    columns = inspector.get_columns(TABLE_NAME)

    print("\nTable Schema:")
    for column in columns:
        print(f"{column['name']} - {column['type']}")