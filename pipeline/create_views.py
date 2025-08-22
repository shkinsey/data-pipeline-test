"""
Materialized view creation module for ETL pipeline.

This module creates the core materialized views required by the Customer Success
and Finance teams as specified in the technical test requirements. These views
provide pre-aggregated data that allows teams to perform simple SELECT queries
without complex grouping or aggregation operations.

Business Requirements:
- Customer Success: Track user engagement, activity patterns, and system usage
- Finance: Calculate total credit balances per organization for invoicing

Technical Features:
- Materialized views for improved query performance
- Unique indexes for data integrity and fast lookups
- Proper error handling and transaction management
- PostgreSQL-specific optimizations

Author: Data Pipeline Team
Version: 1.0
"""

from sqlalchemy import create_engine, text
from config import DB_URL
import traceback

def create_cs_view(view_name: str) -> bool:
    """
    Create materialized view for Customer Success team requirements.
    
    Creates a comprehensive daily activity view that aggregates user actions
    by date, organization, user, and action type. This allows the Customer Success
    team to understand who is using the system, for what purpose, and when,
    without needing to write complex SQL queries.
    
    View Features:
    - Daily activity aggregation with date grouping
    - Credit usage tracking by action type (add/deduct)
    - User engagement classification (High/Medium/Low/No Usage)
    - Action count metrics for activity analysis
    - Data quality filters to exclude incomplete records
    
    SQL Logic:
    - Groups by activity_date, org_id, user_id, action, credit_type
    - Sums credits based on action type for usage tracking
    - Counts actions to determine engagement levels
    - Filters out NULL values and default credit types
    - Orders by date DESC for recent activity first
    
    Performance Optimizations:
    - Unique composite index on grouping columns
    - Materialized view for fast query execution
    - Proper WHERE clause filtering for data quality
    
    Args:
        view_name (str): Name for the materialized view (e.g., 'customer_success_daily_activity')
        
    Returns:
        bool: True if view created successfully, False otherwise
        
    Raises:
        Exception: Database connection or SQL execution errors
        
    Example:
        >>> if create_cs_view("customer_success_daily_activity"):
        ...     print("Customer Success view ready for queries")
        
    Note:
        This view supports the technical test requirement for Customer Success
        to track "who is using system, for what, and when" with simple SELECT queries.
    """
    try:
        engine = create_engine(DB_URL)

        # Create materialized view with comprehensive user activity aggregation
        CREATE_CS_MV = text(f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS
            SELECT
                DATE(timestamp) AS activity_date,
                org_id,
                user_id,
                action,
                credit_type,
                -- Calculate total credits used/added based on action type
                SUM(
                    CASE 
                        WHEN action = 'add' THEN credits
                        WHEN action = 'deduct' THEN credits
                        ELSE credits
                    END
                ) AS total_credits_used,
                COUNT(*) AS action_count,
                -- User engagement classification for Customer Success analysis
                CASE 
                    WHEN COUNT(*) >= 10 THEN 'High Usage'
                    WHEN COUNT(*) >= 5 THEN 'Medium Usage'
                    WHEN COUNT(*) >= 1 THEN 'Low Usage'
                    ELSE 'No Usage'
                END AS usage_level
            FROM
                user_actions
            WHERE
                -- Data quality filters to ensure clean analytics
                user_id IS NOT NULL 
                AND org_id IS NOT NULL
                AND action IS NOT NULL
                AND credit_type != 'default'  -- Exclude default/placeholder values
            GROUP BY
                activity_date, org_id, user_id, action, credit_type
            ORDER BY
                activity_date DESC, org_id, user_id;
        """)

        # Create unique index for performance and data integrity
        CREATE_CS_MV_INDEX = text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_cs_daily_activity_unique
            ON {view_name} (activity_date, org_id, user_id, action, credit_type);
        """)

        with engine.connect() as conn:
            # Drop existing view to ensure clean recreation
            conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {view_name};"))
            conn.execute(CREATE_CS_MV)
            conn.execute(CREATE_CS_MV_INDEX)
            conn.commit()

        return True
    except Exception:
        print(f"Failed to create Customer Success view '{view_name}':")
        print(traceback.format_exc())
        return False
    

def create_finance_view(view_name: str) -> bool:
    """
    Create materialized view for Finance team requirements.
    
    Creates a credit balance summary view that calculates total credit balances
    per organization for invoicing purposes. This allows the Finance team to
    quickly determine how much each organization owes or has in credits without
    complex aggregation queries.
    
    View Features:
    - Organization-level credit balance aggregation
    - Proper credit/debit accounting logic
    - Simple structure for easy SELECT queries
    - Optimized for invoicing workflows
    
    SQL Logic:
    - Groups all user actions by org_id
    - Applies accounting rules: 'add' = positive, 'deduct' = negative
    - Sums to get net credit balance per organization
    - Positive balance = credits available, Negative = amount owed
    
    Business Logic:
    - 'add' actions increase organization's credit balance
    - 'deduct' actions decrease organization's credit balance
    - Final total_credits represents net balance for invoicing
    
    Performance Optimizations:
    - Unique index on org_id for fast organization lookups
    - Materialized view for instant balance queries
    - Simple aggregation structure for optimal performance
    
    Args:
        view_name (str): Name for the materialized view (e.g., 'finance_org_credit_balance')
        
    Returns:
        bool: True if view created successfully, False otherwise
        
    Raises:
        Exception: Database connection or SQL execution errors
        
    Example:
        >>> if create_finance_view("finance_org_credit_balance"):
        ...     print("Finance view ready for invoicing queries")
        
    Note:
        This view supports the technical test requirement for Finance team
        to get "total credit balance per organization for invoicing" with simple SELECT queries.
    """
    try:
        engine = create_engine(DB_URL)

        # Create materialized view with organization credit balance calculation
        CREATE_FINANCE_MV = text(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS
        SELECT
            org_id,
            -- Calculate net credit balance using accounting principles
            SUM(
                CASE 
                    WHEN action = 'add' THEN credits      -- Credits added to account
                    WHEN action = 'deduct' THEN -credits  -- Credits deducted from account
                    ELSE 0                                -- Handle any other action types
                END
            ) AS total_credits
        FROM
            user_actions
        GROUP BY
            org_id;
        """)

        # Create unique index for fast organization lookups
        CREATE_FINANCE_MV_INDEX = text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_finance_org_credit_balance_org_id
            ON {view_name} (org_id);
        """)

        with engine.connect() as conn:
            # Drop existing view to ensure clean recreation
            conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {view_name};"))
            conn.execute(CREATE_FINANCE_MV)
            conn.execute(CREATE_FINANCE_MV_INDEX)
            conn.commit()

        return True
    
    except Exception:
        print(f"Failed to create Finance view '{view_name}':")
        print(traceback.format_exc())
        return False