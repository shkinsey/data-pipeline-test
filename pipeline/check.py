import pandas as pd
from sqlalchemy import create_engine, inspect
from config import DB_URL

def check_result(table_name: str):
    engine = create_engine(DB_URL)

    result_df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
    print(result_df)

    # Print the schema (column names and types)
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)

    print("\nTable Schema:")
    for column in columns:
        print(f"{column['name']} - {column['type']}")
