"""
Materialized view validation module for ETL pipeline.

This module provides functions to refresh and validate materialized views,
handling both original views (with concurrent refresh) and enhanced views
(freshly created). It includes intelligent fallback mechanisms for different
PostgreSQL refresh strategies.

Author: Data Pipeline Team
Version: 1.0
"""

from sqlalchemy import create_engine, text
from config import DB_URL
import pandas as pd
import traceback
from typing import Optional


def check_mat_view(view_name: str) -> Optional[pd.DataFrame]:
    """
    Refresh and validate materialized view data with intelligent fallback strategies.
    
    This function handles different types of materialized views with appropriate
    refresh strategies. Enhanced views (freshly created) are queried directly,
    while original views attempt concurrent refresh with fallback to regular refresh.
    
    Refresh Strategies:
    - Enhanced/Executive views: Direct query (no refresh needed)
    - Original views: Concurrent refresh → Regular refresh fallback
    - Displays sample results for validation
    
    Args:
        view_name (str): Name of the materialized view to check
        
    Returns:
        Optional[pd.DataFrame]: Query results if successful, None if failed
        
    Raises:
        Exception: Database connection or query execution errors
        
    Example:
        >>> result = check_mat_view("enhanced_customer_success_view")
        enhanced_customer_success_view results:
        [displays first 10 rows]
        Total rows: 150
        
    Note:
        Concurrent refresh requires unique indexes on materialized views.
        Enhanced views don't have these indexes, so they skip refresh entirely.
    """
    try:
        engine = create_engine(DB_URL)

        # SQL query to get view data
        exec_view = text(f"SELECT * FROM {view_name};")

        # Determine refresh strategy based on view type
        if "enhanced" in view_name or "executive" in view_name:
            # Enhanced views are freshly created - no refresh needed
            # Query directly for immediate results
            result_df = pd.read_sql_query(exec_view, engine)
        else:
            # Original views - attempt concurrent refresh with fallback
            try:
                # Try concurrent refresh first (requires unique indexes)
                refresh_mv = text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name};")
                with engine.connect() as conn:
                    conn.execute(refresh_mv)
                    conn.commit()
            except Exception:
                # Fallback to regular refresh if concurrent fails
                refresh_mv = text(f"REFRESH MATERIALIZED VIEW {view_name};")
                with engine.connect() as conn:
                    conn.execute(refresh_mv)
                    conn.commit()
            
            # Query the refreshed view
            result_df = pd.read_sql_query(exec_view, engine)

        # Display validation results with reasonable output limits
        print(f"\n{view_name} results:")
        print(result_df.head(10).to_string(index=False))  # Show first 10 rows
        print(f"Total rows: {len(result_df)}")
        
        # Basic data quality validation
        if len(result_df) == 0:
            print(f"⚠️  Warning: {view_name} contains no data")
        else:
            print(f"✅ {view_name} validation passed")
        
        return result_df

    except Exception:
        print(f"❌ Failed to check materialized view {view_name}:")
        print(traceback.format_exc())
        return None
