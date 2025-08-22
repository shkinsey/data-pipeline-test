"""
Data loading module for ETL pipeline.

This module handles the final stage of the ETL process by creating the target
database table and loading transformed data into PostgreSQL. It ensures proper
table structure and handles data type conversions for reliable data storage.

Author: Data Pipeline Team
Version: 1.0
"""

from sqlalchemy import create_engine, text
import pandas as pd
from config import DB_URL
import traceback


def load(df: pd.DataFrame, table_name: str) -> None:
    """
    Load transformed DataFrame into PostgreSQL database table.
    
    Creates the target table with appropriate schema and loads the cleaned
    data from the transformation stage. The table is recreated each time
    to ensure data consistency and handle schema changes.
    
    Table Schema:
    - org_id: Organization identifier (TEXT)
    - user_id: User identifier (TEXT) 
    - credit_type: Type of credit transaction (TEXT)
    - action: Action performed (add/deduct) (TEXT)
    - credits: Credit amount (DOUBLE PRECISION)
    - timestamp: Transaction timestamp (TIMESTAMP WITH TIME ZONE)
    
    Args:
        df (pd.DataFrame): Transformed DataFrame containing cleaned data
        table_name (str): Name of the target database table
        
    Returns:
        None
        
    Raises:
        Exception: Database connection or SQL execution errors
        
    Example:
        >>> transformed_data = transform(raw_data)
        >>> load(transformed_data, "user_actions")
        # Data is now available in PostgreSQL for analysis
        
    Note:
        This function drops and recreates the table to ensure clean state.
        Use CASCADE to handle dependent views properly.
    """
    try:
        engine = create_engine(DB_URL)

        # Define table schema optimized for the transformed data structure
        create_table = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                org_id TEXT NOT NULL,                                    -- Organization identifier
                user_id TEXT NOT NULL,                                   -- User identifier  
                credit_type TEXT,                                        -- Credit transaction type
                action TEXT,                                             -- Action: add or deduct
                credits DOUBLE PRECISION,                                -- Credit amount (numeric)
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP  -- Transaction timestamp
            );
        """
        
        with engine.connect() as conn:
            # Drop existing table with CASCADE to handle dependent views
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
            
            # Create fresh table with proper schema
            conn.execute(text(create_table))
            conn.commit()
        
        # Load DataFrame into PostgreSQL using pandas built-in method
        # if_exists='append' works with the fresh table created above
        df.to_sql(table_name, engine, if_exists='append', index=False)
        
        print(f"✅ Successfully loaded {len(df)} records into {table_name}")

    except Exception:
        print(f"❌ Failed to load data into {table_name}:")
        print(traceback.format_exc())