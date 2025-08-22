import pandas as pd
import traceback

# Extract function to read CSV data into a DataFrame
def extract(csv_path):
    
    try:
        return pd.read_csv(csv_path, parse_dates=['timestamp'])
    
    except Exception:
        print(traceback.format_exc())
        raise
    

