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
    Create enhanced Customer Success materialized view with high-level user analytics.

    This view provides comprehensive user-level insights for customer success teams,
    aggregating all activity per user to identify engagement patterns, credit usage,
    and account health status.

    Features:
    - User-level aggregation (not per-action granularity)
    - Net credit balance and usage patterns per user
    - Engagement categorization (Power/Active/Regular/Light User)
    - Customer health status (At Risk, Healthy, Upsell Opportunity)
    - Activity timeline and usage efficiency metrics
    - Actionable insights for customer success interventions

    Args:
        view_name (str): Name for the materialized view to create

    Returns:
        bool: True if view created successfully, False otherwise

    Raises:
        Exception: Database connection or SQL execution errors

    Example:
        >>> success = create_enhanced_cs_view("enhanced_customer_success_view")
        >>> if success:
        ...     print("High-level CS analytics view created successfully")
    """
    try:
        engine = create_engine(DB_URL)

        CREATE_ENHANCED_CS_MV = text(f"""
            CREATE MATERIALIZED VIEW {view_name} AS
            SELECT
                -- Organization details
                o.org_name AS organization,
                o.industry,
                
                -- User details
                u.user_name AS user_name,
                u.role AS user_role,
                u.email AS user_email,
                
                -- Activity timeline
                MIN(DATE(ua.timestamp)) AS first_activity_date,
                MAX(DATE(ua.timestamp)) AS last_activity_date,
                COUNT(DISTINCT DATE(ua.timestamp)) AS active_days,
                
                -- Credit metrics (net balance per user)
                SUM(
                    CASE 
                        WHEN ua.action = 'add' THEN ua.credits
                        WHEN ua.action = 'deduct' THEN -ua.credits
                        ELSE 0
                    END
                ) AS net_credit_balance,
                
                SUM(
                    CASE WHEN ua.action = 'add' THEN ua.credits ELSE 0 END
                ) AS total_credits_purchased,
                
                SUM(
                    CASE WHEN ua.action = 'deduct' THEN ua.credits ELSE 0 END
                ) AS total_credits_consumed,
                
                -- Activity metrics
                COUNT(*) AS total_actions,
                COUNT(CASE WHEN ua.action = 'add' THEN 1 END) AS purchase_actions,
                COUNT(CASE WHEN ua.action = 'deduct' THEN 1 END) AS usage_actions,
                
                -- Engagement categorization (based on total activity)
                CASE 
                    WHEN COUNT(*) >= 50 THEN 'Power User'
                    WHEN COUNT(*) >= 20 THEN 'Active User'
                    WHEN COUNT(*) >= 5 THEN 'Regular User'
                    WHEN COUNT(*) >= 1 THEN 'Light User'
                    ELSE 'Inactive'
                END AS engagement_level,
                
                -- Customer success metrics
                CASE 
                    WHEN MAX(DATE(ua.timestamp)) < CURRENT_DATE - INTERVAL '30 days' THEN 'At Risk - Inactive'
                    WHEN SUM(CASE WHEN ua.action = 'deduct' THEN ua.credits ELSE 0 END) = 0 THEN 'Not Using Credits'
                    WHEN SUM(CASE WHEN ua.action = 'add' THEN ua.credits WHEN ua.action = 'deduct' THEN -ua.credits ELSE 0 END) < 0 THEN 'Low Balance - Upsell Opportunity'
                    ELSE 'Healthy'
                END AS customer_status,
                
                -- Usage efficiency
                ROUND(
                    (SUM(CASE WHEN ua.action = 'deduct' THEN ua.credits ELSE 0 END) / 
                     NULLIF(COUNT(DISTINCT DATE(ua.timestamp)), 0))::numeric, 2
                ) AS avg_daily_usage
            FROM
                user_actions ua
                INNER JOIN organizations o ON ua.org_id = o.org_id
                INNER JOIN users u ON ua.user_id = u.user_id
            WHERE
                ua.user_id IS NOT NULL 
                AND ua.org_id IS NOT NULL
                AND ua.action IS NOT NULL
                AND ua.credit_type != 'default'
            GROUP BY
                o.org_name, o.industry, u.user_name, u.role, u.email, ua.user_id
            ORDER BY
                total_actions DESC, o.org_name, u.user_name;
        """)

        # Performance indexes for common query patterns
        CREATE_ENHANCED_CS_MV_INDEX = text(f"""
            CREATE INDEX IF NOT EXISTS idx_enhanced_cs_activity_date
            ON {view_name} (first_activity_date);
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
