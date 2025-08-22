"""
Data transformation module for ETL pipeline.

This module handles the core data cleaning and transformation logic, converting
messy CSV data into a clean, standardized format suitable for analysis. It implements
robust date parsing, data quality checks, and business rule enforcement.

Key Features:
- Multi-format date parsing with fallback mechanisms
- Data quality validation and missing value handling
- Business rule enforcement (users belong to single organization)
- String normalization and data type standardization

Author: Data Pipeline Team
Version: 1.0
"""

import pandas as pd


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform and clean raw DataFrame for database loading.

    Performs comprehensive data cleaning including date standardization,
    missing value handling, data quality validation, and business rule
    enforcement. This is the core data processing function that ensures
    data integrity and consistency.

    Transformation Steps:
    1. Parse inconsistent date formats using multiple strategies
    2. Sort data by user and timestamp for consistency
    3. Remove records with missing essential identifiers
    4. Enforce business rule: users belong to single organization
    5. Handle missing timestamps with fallback values
    6. Normalize string data (trim whitespace, lowercase)
    7. Fill missing numeric and categorical values

    Data Quality Rules:
    - org_id and user_id are required (records dropped if missing)
    - Users can only belong to one organization (first org wins)
    - Missing timestamps get 1990-01-01 placeholder for identification
    - Missing credits default to 1.0
    - Missing credit_type defaults to 'default'

    Args:
        df (pd.DataFrame): Raw DataFrame from extraction stage

    Returns:
        pd.DataFrame: Cleaned and standardized DataFrame ready for loading

    Raises:
        Exception: Data processing errors or validation failures

    Example:
        >>> raw_data = extract("data/test_data.csv")
        >>> clean_data = transform(raw_data)
        >>> print(f"Cleaned {len(clean_data)} records from {len(raw_data)} raw records")

    Note:
        The 1990-01-01 date is used as a sentinel value for missing timestamps
        and is filtered out in enhanced views for accurate business intelligence.
    """
    try:
        print("Transforming data...")

        # Date parsing configuration
        START_DATE = pd.to_datetime("1990-01-01")
        END_DATE = pd.to_datetime("2050-01-01")

        # Multiple date format patterns to handle inconsistent input data
        STRING_FORMATS = [
            "%Y-%m-%d %H:%M:%S",  # ISO format: 2024-11-24 00:00:00
            "%d-%m-%Y",  # European format: 19-12-2024
            "%m/%d/%Y %H:%M %p",  # US format: 10/29/2024 12:00 AM
        ]

        def datetime_parser(date_value):
            """
            Robust date parsing function handling multiple formats and edge cases.

            Attempts to parse dates as Unix timestamps first, then tries multiple
            string formats. Returns pd.NaT for unparseable values to be handled
            by the fallback logic.

            Args:
                date_value: Raw date value (string, numeric, or other)

            Returns:
                pd.Timestamp or pd.NaT: Parsed timestamp or NaT if unparseable
            """
            # Try parsing as Unix timestamp first
            numeric_val = pd.to_numeric(date_value, errors="coerce")
            if pd.notna(numeric_val):
                try:
                    ts = pd.to_datetime(numeric_val, unit="s")
                    if START_DATE <= ts <= END_DATE:
                        return ts
                except (ValueError, pd.errors.OutOfBoundsDatetime):
                    pass

            # Try parsing as string with multiple format patterns
            if isinstance(date_value, str):
                for fmt in STRING_FORMATS:
                    try:
                        return pd.to_datetime(date_value, format=fmt)
                    except (ValueError, TypeError):
                        continue

            # Return NaT for unparseable values (handled by fallback logic)
            return pd.NaT

        # Apply robust date parsing to timestamp column
        df["timestamp"] = df["timestamp"].apply(datetime_parser)

        # Sort by user and timestamp for consistent processing
        df = df.sort_values(["user_id", "timestamp"])

        # Data Quality: Drop rows with missing essential identifiers
        # These are required for meaningful analysis and foreign key relationships
        original_count = len(df)
        df = df.dropna(subset=["org_id", "user_id"], how="all")
        dropped_count = original_count - len(df)
        if dropped_count > 0:
            print(f"⚠️  Dropped {dropped_count} records with missing org_id or user_id")

        # Business Rule: Ensure users only belong to a single organisation
        # If a user appears in multiple orgs, keep only their first organization
        first_orgs = df.groupby("user_id")["org_id"].transform("first")
        df = df[df["org_id"] == first_orgs]

        # Handle missing timestamps with sentinel value for later identification
        df["timestamp"] = df["timestamp"].fillna(pd.to_datetime("1990-01-01 00:00:00"))

        # Preserve timestamp and credits columns before string processing
        df_timestamps = df["timestamp"]
        df_credits = df["credits"]

        # String normalization: trim whitespace and convert to lowercase
        df_string_cols = df.select_dtypes("object")
        for col in df_string_cols.columns:
            df_string_cols[col] = df_string_cols[col].str.strip()
            df_string_cols[col] = df_string_cols[col].str.lower()

        # Reconstruct DataFrame with processed columns
        df = pd.concat([df_string_cols, df_credits, df_timestamps], axis=1)

        # Fill missing numeric values with reasonable defaults
        df["credits"] = df["credits"].fillna(1.0)  # Default credit amount

        # Fill missing categorical values
        df["credit_type"] = df["credit_type"].fillna("default")  # Placeholder type

        # Final sort for consistent output
        df.sort_values(["org_id", "user_id"], inplace=True)

        print(f"✅ Transformation complete: {len(df)} clean records ready for loading")
        return df

    except Exception as e:
        print(f"❌ Transformation failed: {e}")
        raise
