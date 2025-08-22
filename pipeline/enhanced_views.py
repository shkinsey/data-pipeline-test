"""
Enhanced materialized views module for normalized database structure.

This module creates advanced materialized views that demonstrate table joins
and provide business intelligence features beyond the basic requirements.
All views use meaningful organization and user names instead of cryptic IDs.

Author: Data Pipeline Team
Version: 1.0
"""

from sqlalchemy import create_engine, text
from config import DB_URL
import traceback


def create_enhanced_cs_view(view_name: str) -> bool:
    """
    Create enhanced Customer Success materialized view with normalized data.

    This view demonstrates advanced SQL joins across three tables (user_actions,
    organizations, users) and provides detailed user engagement analytics with
    meaningful names instead of IDs.

    Features:
    - Organization names and industry classifications
    - User names, roles, and contact information
    - Engagement level categorization (High/Medium/Low Usage)
    - Credit efficiency metrics per action
    - Daily activity aggregation

    Args:
        view_name (str): Name for the materialized view to create

    Returns:
        bool: True if view created successfully, False otherwise

    Raises:
        Exception: Database connection or SQL execution errors

    Example:
        >>> success = create_enhanced_cs_view("enhanced_customer_success_view")
        >>> if success:
        ...     print("Enhanced CS view created successfully")
    """
    try:
        engine = create_engine(DB_URL)

        CREATE_ENHANCED_CS_MV = text(f"""
            CREATE MATERIALIZED VIEW {view_name} AS
            SELECT
                -- Time dimension
                DATE(ua.timestamp) AS activity_date,
                
                -- Organization details (from normalized table)
                o.org_name AS organization,
                o.industry,
                
                -- User details (from normalized table)
                u.user_name AS user_name,
                u.role AS user_role,
                u.email AS user_email,
                
                -- Action details
                ua.action,
                ua.credit_type,
                
                -- Credit calculations (note: both add/deduct show positive for usage tracking)
                SUM(
                    CASE 
                        WHEN ua.action = 'add' THEN ua.credits
                        WHEN ua.action = 'deduct' THEN -ua.credits
                        ELSE ua.credits
                    END
                ) AS total_credits_used,
                
                -- Activity metrics
                COUNT(*) AS action_count,
                
                -- Business intelligence: User engagement categorization
                CASE 
                    WHEN COUNT(*) >= 10 THEN 'High Usage'
                    WHEN COUNT(*) >= 5 THEN 'Medium Usage'
                    WHEN COUNT(*) >= 1 THEN 'Low Usage'
                    ELSE 'No Usage'
                END AS usage_level,
                
                -- Efficiency metric: Average credits per action (PostgreSQL numeric casting)
                ROUND(
                    (SUM(ua.credits) / NULLIF(COUNT(*), 0))::numeric, 2
                ) AS avg_credits_per_action
            FROM
                user_actions ua
                INNER JOIN organizations o ON ua.org_id = o.org_id
                INNER JOIN users u ON ua.user_id = u.user_id
            WHERE
                -- Data quality filters
                ua.user_id IS NOT NULL 
                AND ua.org_id IS NOT NULL
                AND ua.action IS NOT NULL
                AND ua.credit_type != 'default'  -- Exclude placeholder credit types
            GROUP BY
                activity_date, o.org_name, o.industry, u.user_name, u.role, u.email, ua.action, ua.credit_type
            ORDER BY
                activity_date DESC, o.org_name, u.user_name;
        """)

        # Performance indexes for common query patterns
        CREATE_ENHANCED_CS_MV_INDEX = text(f"""
            CREATE INDEX IF NOT EXISTS idx_enhanced_cs_activity_date
            ON {view_name} (activity_date);
        """)

        CREATE_ENHANCED_CS_MV_INDEX2 = text(f"""
            CREATE INDEX IF NOT EXISTS idx_enhanced_cs_organization
            ON {view_name} (organization);
        """)

        with engine.connect() as conn:
            # Clean removal: Drop indexes first, then view with CASCADE
            conn.execute(text("DROP INDEX IF EXISTS idx_enhanced_cs_activity_date;"))
            conn.execute(text("DROP INDEX IF EXISTS idx_enhanced_cs_organization;"))
            conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;"))

            # Create the view
            conn.execute(CREATE_ENHANCED_CS_MV)

            # Create performance indexes after the view
            conn.execute(CREATE_ENHANCED_CS_MV_INDEX)
            conn.execute(CREATE_ENHANCED_CS_MV_INDEX2)
            conn.commit()

        return True
    except Exception:
        print(traceback.format_exc())
        return False


def create_enhanced_finance_view(view_name: str) -> bool:
    """
    Create enhanced Finance materialized view with detailed credit analysis.

    This view provides comprehensive financial analytics for invoicing and
    credit management, with meaningful organization names and detailed
    breakdowns of credit transactions.

    Features:
    - Net credit balance calculation (add = positive, deduct = negative)
    - Separate totals for credits added vs. used
    - Active user counts per organization
    - Invoice status recommendations based on balance
    - Transaction volume analysis
    - Industry-based grouping

    Args:
        view_name (str): Name for the materialized view to create

    Returns:
        bool: True if view created successfully, False otherwise

    Raises:
        Exception: Database connection or SQL execution errors

    Example:
        >>> success = create_enhanced_finance_view("enhanced_finance_view")
        >>> if success:
        ...     print("Enhanced Finance view ready for invoicing")
    """
    try:
        engine = create_engine(DB_URL)

        CREATE_ENHANCED_FINANCE_MV = text(f"""
        CREATE MATERIALIZED VIEW {view_name} AS
        SELECT
            -- Organization details (meaningful names instead of IDs)
            o.org_name AS organization,
            o.industry,
            o.org_id,  -- Keep ID for technical references
            
            -- Net credit balance: positive = customer owes us, negative = we owe customer
            SUM(
                CASE 
                    WHEN ua.action = 'add' THEN ua.credits      -- Credits purchased/added
                    WHEN ua.action = 'deduct' THEN -ua.credits  -- Credits consumed/used
                    ELSE 0
                END
            ) AS net_credit_balance,
            
            -- Detailed credit breakdown for financial analysis
            SUM(
                CASE 
                    WHEN ua.action = 'add' THEN ua.credits
                    ELSE 0
                END
            ) AS total_credits_added,
            
            SUM(
                CASE 
                    WHEN ua.action = 'deduct' THEN ua.credits
                    ELSE 0
                END
            ) AS total_credits_used,
            
            -- User engagement metrics
            COUNT(DISTINCT ua.user_id) AS active_users,
            COUNT(*) AS total_transactions,
            
            -- Business intelligence: Invoice status automation
            CASE 
                WHEN SUM(
                    CASE 
                        WHEN ua.action = 'add' THEN ua.credits
                        WHEN ua.action = 'deduct' THEN -ua.credits
                        ELSE 0
                    END
                ) > 0 THEN 'In Credit - No Action Required'
                WHEN SUM(
                    CASE 
                        WHEN ua.action = 'add' THEN ua.credits
                        WHEN ua.action = 'deduct' THEN -ua.credits
                        ELSE 0
                    END
                ) < 0 THEN 'In Debit - Invoice Required'
                ELSE 'Balanced - No Action Required'
            END AS invoice_status,
            
            -- Financial KPI: Average transaction value (PostgreSQL numeric casting)
            ROUND(
                AVG(ua.credits)::numeric, 2
            ) AS avg_transaction_value
        FROM
            user_actions ua
            INNER JOIN organizations o ON ua.org_id = o.org_id
        GROUP BY
            o.org_name, o.industry, o.org_id
        ORDER BY
            net_credit_balance DESC;  -- Highest balances first for priority invoicing
        """)

        # Performance indexes for financial queries
        CREATE_ENHANCED_FINANCE_MV_INDEX = text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_enhanced_finance_org_id
            ON {view_name} (org_id);
        """)

        CREATE_ENHANCED_FINANCE_MV_INDEX2 = text(f"""
            CREATE INDEX IF NOT EXISTS idx_enhanced_finance_balance
            ON {view_name} (net_credit_balance);
        """)

        with engine.connect() as conn:
            # Clean removal: Drop indexes first, then view with CASCADE
            conn.execute(text("DROP INDEX IF EXISTS idx_enhanced_finance_org_id;"))
            conn.execute(text("DROP INDEX IF EXISTS idx_enhanced_finance_balance;"))
            conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;"))

            # Create the view
            conn.execute(CREATE_ENHANCED_FINANCE_MV)

            # Create performance indexes after the view
            conn.execute(CREATE_ENHANCED_FINANCE_MV_INDEX)
            conn.execute(CREATE_ENHANCED_FINANCE_MV_INDEX2)
            conn.commit()

        return True

    except Exception:
        print(traceback.format_exc())
        return False


def create_executive_summary_view(view_name: str) -> bool:
    """
    Create executive summary materialized view for high-level business intelligence.

    This view provides C-level executives with key performance indicators and
    insights across all organizations, featuring advanced SQL techniques like
    subqueries and window functions.

    Features:
    - Organization-level KPIs and metrics
    - Most active users and primary action types per org
    - Activity date ranges (excluding dummy 1990 dates)
    - Credit volume analysis
    - User engagement statistics
    - Industry-based insights

    Args:
        view_name (str): Name for the materialized view to create

    Returns:
        bool: True if view created successfully, False otherwise

    Raises:
        Exception: Database connection or SQL execution errors

    Example:
        >>> success = create_executive_summary_view("executive_summary_view")
        >>> if success:
        ...     print("Executive dashboard data ready")
    """
    try:
        engine = create_engine(DB_URL)

        CREATE_EXEC_SUMMARY_MV = text(f"""
        CREATE MATERIALIZED VIEW {view_name} AS
        SELECT
            -- Organization identification
            o.org_name AS organization,
            o.industry,
            
            -- User engagement KPIs
            COUNT(DISTINCT u.user_id) AS total_users,
            COUNT(DISTINCT DATE(ua.timestamp)) AS active_days,
            
            -- Financial KPIs
            SUM(ua.credits) AS total_credit_volume,
            COUNT(*) AS total_actions,
            ROUND(AVG(ua.credits)::numeric, 2) AS avg_credit_per_action,
            
            -- Business intelligence: Most common action type per organization
            -- Uses correlated subquery to find the action with highest frequency
            (SELECT ua2.action 
             FROM user_actions ua2 
             WHERE ua2.org_id = o.org_id 
             GROUP BY ua2.action 
             ORDER BY COUNT(*) DESC 
             LIMIT 1) AS primary_action_type,
            
            -- Business intelligence: Most active user per organization
            -- Demonstrates JOIN within subquery for meaningful names
            (SELECT u2.user_name 
             FROM user_actions ua3
             INNER JOIN users u2 ON ua3.user_id = u2.user_id
             WHERE ua3.org_id = o.org_id 
             GROUP BY u2.user_name 
             ORDER BY COUNT(*) DESC 
             LIMIT 1) AS most_active_user,
            
            -- Activity timeline (excluding dummy 1990 dates from data cleaning)
            MIN(CASE WHEN DATE(ua.timestamp) > '1990-01-01' THEN DATE(ua.timestamp) END) AS first_activity,
            MAX(DATE(ua.timestamp)) AS last_activity
        FROM
            user_actions ua
            INNER JOIN organizations o ON ua.org_id = o.org_id
            INNER JOIN users u ON ua.user_id = u.user_id
        WHERE
            ua.credit_type != 'default'  -- Exclude placeholder data from transformation
        GROUP BY
            o.org_name, o.industry, o.org_id
        ORDER BY
            total_credit_volume DESC;  -- Highest volume organizations first
        """)

        with engine.connect() as conn:
            # Clean removal with CASCADE to handle any dependencies
            conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;"))
            conn.execute(CREATE_EXEC_SUMMARY_MV)
            conn.commit()

        return True

    except Exception:
        print(traceback.format_exc())
        return False
