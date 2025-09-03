"""
Data extraction module for ETL pipeline.

This module handles the initial stage of the ETL process by reading raw CSV data
and performing basic data type inference. It provides robust error handling and
supports various CSV formats with automatic date parsing attempts.

Author: Data Pipeline Team
Version: 1.0
"""

import pandas as pd
import traceback


def extract(csv_path: str) -> pd.DataFrame:
    """
    Extract data from CSV file with automatic date parsing.
    
    Reads the raw CSV data and attempts to parse timestamp columns automatically.
    This function handles the messy input data that contains inconsistent date
    formats and missing values, preparing it for the transformation stage.
    
    Data Quality Expectations:
    - CSV file contains user action data with timestamps
    - May have inconsistent date formats requiring transformation
    - May contain missing or malformed data requiring cleaning
    - Column names: org_id, user_id, credit_type, action, credits, timestamp
    
    Args:
        csv_path (str): Path to the CSV file to extract
        
    Returns:
        DataFrame: Raw DataFrame with basic date parsing applied
        
    Raises:
        Exception: File not found, CSV parsing errors, or data access issues
        
    Example:
        >>> raw_data = extract("data/test_data.csv")
        >>> print(f"Extracted {len(raw_data)} records")
        >>> print(raw_data.dtypes)  # Check data types
        
    Note:
        Date parsing may fail for some records due to inconsistent formats.
        The transform stage will handle these parsing failures robustly.
    """
    try:
        # Attempt to parse timestamp column during CSV reading
        # This provides a first pass at date parsing, with fallbacks in transform
        return pd.read_csv(csv_path, parse_dates=['timestamp'])
    
    except Exception:
        print(f"❌ Failed to extract data from {csv_path}:")
        print(traceback.format_exc())
        raise
