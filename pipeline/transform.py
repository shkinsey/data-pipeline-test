from config import TABLE_NAME
import pandas as pd

# Transform function to process the DataFrame
def transform(df: pd.DataFrame):
    try:
        print("Transforming data...")

        START_DATE = pd.to_datetime('1990-01-01')
        END_DATE = pd.to_datetime('2050-01-01')

        STRING_FORMATS = [
            '%Y-%m-%d %H:%M:%S',
            '%d-%m-%Y',
            '%m/%d/%Y %H:%M %p'
        ]

        def datetime_parser(date_value):
            """
            A single, efficient function to parse a value that could be
            a Unix timestamp or one of several string formats.
            """
            numeric_val = pd.to_numeric(date_value, errors='coerce')
            if pd.notna(numeric_val):
                try:
                    ts = pd.to_datetime(numeric_val, unit='s')
                    if START_DATE <= ts <= END_DATE:
                        return ts
                except (ValueError, pd.errors.OutOfBoundsDatetime):
                    pass

            if isinstance(date_value, str):
                for fmt in STRING_FORMATS:
                    try:
                        return pd.to_datetime(date_value, format=fmt)
                    except (ValueError, TypeError):
                        continue

            return pd.NaT

        # Parse dates to a consistent format
        df['timestamp'] = df['timestamp'].apply(datetime_parser)

        df = df.sort_values(['user_id', 'timestamp'])

        # Drop rows with missing essential fields 
        df = df.dropna(subset=['org_id', 'user_id'])

        # Ensure users only belong to a single organisation
        first_orgs = df.groupby('user_id')['org_id'].transform('first')
        df = df[df['org_id'] == first_orgs]

        df['timestamp'] = df['timestamp'].fillna(pd.to_datetime('1990-01-01 00:00:00'))

        df_timestamps = df['timestamp']

        df_credits = df['credits']

        # Trim whitespace and normalise strings
        df_string_cols = df.select_dtypes('object')
        for col in df_string_cols.columns:
            df_string_cols[col] = df_string_cols[col].str.strip()
            df_string_cols[col] = df_string_cols[col].str.lower()
        df_string_cols

        df = pd.concat([df_string_cols, df_credits, df_timestamps], axis=1)
        
        # Fill missing numeric fields
        df['credits'] = df['credits'].fillna(1.0)

        df['credit_type'] = df['credit_type'].fillna('default')

        df.sort_values(['org_id', 'user_id'], inplace=True)
            
        return df
    
    except Exception as e:
        print(f"An error occurred: {e}")