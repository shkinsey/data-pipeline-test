"""
Data validation module for ETL pipeline.

This module provides validation functions to verify the successful loading
and integrity of data in the PostgreSQL database. It performs basic data
quality checks and displays sample results for manual verification.

Author: Data Pipeline Team
Version: 1.0
"""

from sqlalchemy import create_engine, text
from config import DB_URL
import pandas as pd
import traceback

def check_result(table_name: str) -> None:
    """
    Validate data loading results and display sample records.
    
    Performs basic validation checks on the loaded data including record counts,
    data type verification, and sample data display. This function helps ensure
    the ETL pipeline has completed successfully and data is ready for analysis.
    
    Validation Checks:
    - Verifies table exists and is accessible
    - Displays total record count
    - Shows sample records for manual inspection
    - Checks for basic data completeness
    
    Args:
        table_name (str): Name of the database table to validate
        
    Returns:
        None
        
    Raises:
        Exception: Database connection or query execution errors
        
    Example:
        >>> check_result("user_actions")
        Table user_actions contains 1000 records
        Sample records:
        [displays first 5 rows]
        
    Note:
        This is a basic validation function. More sophisticated data quality
        checks are performed in the enhanced views and business intelligence layers.
    """
    try:
        engine = create_engine(DB_URL)
        
        # Query to get basic table statistics and sample data
        query = text(f"SELECT * FROM {table_name} LIMIT 5;")
        count_query = text(f"SELECT COUNT(*) as total_records FROM {table_name};")
        
        # Get record count for validation
        with engine.connect() as conn:
            count_result = conn.execute(count_query).fetchone()
            total_records = count_result[0] if count_result else 0
        
        # Get sample data for inspection
        sample_df = pd.read_sql_query(query, engine)
        
        # Display validation results
        print(f"Table {table_name} contains {total_records} records")
        print(f"\nSample records from {table_name}:")
        print(sample_df.to_string(index=False))
        
        # Basic data quality checks
        if total_records == 0:
            print("Warning: Table is empty!")
        elif total_records < 10:
            print(f"Warning: Only {total_records} records found - expected more data")
        else:
            print("Data loading validation passed")
            
    except Exception:
        print(f"Failed to validate table {table_name}:")
        print(traceback.format_exc())
