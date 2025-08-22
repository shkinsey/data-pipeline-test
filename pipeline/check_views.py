from sqlalchemy import create_engine, text
from config import DB_URL
import pandas as pd
import traceback

def check_mat_view(view_name: str):
    try:
        engine = create_engine(DB_URL)

        # refreshes the data and prevents the view from being locked
        refresh_mv = text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name};")
        exec_view = text(f"SELECT * FROM {view_name};")

        with engine.connect() as conn:
            conn.execute(refresh_mv)
            conn.commit()

        # Execute the query after refreshing
        result_df = pd.read_sql_query(exec_view, engine)
        print(f"\n{view_name} results:")
        print(result_df)
        
        return result_df

    except Exception:
        print(traceback.format_exc())
