from sqlalchemy import create_engine, text
from config import DB_URL
import traceback

def create_cs_view(view_name: str) -> bool:
    try:
        engine = create_engine(DB_URL)

        # Create or replace view for easier view management
        CREATE_CS_MV = text(f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS
            SELECT
                DATE(timestamp) AS activity_date,
                org_id,
                user_id,
                action,
                credit_type,
                SUM(
                    CASE 
                        WHEN action = 'add' THEN credits
                        WHEN action = 'deduct' THEN credits
                        ELSE credits
                    END
                ) AS total_credits_used,
                COUNT(*) AS action_count,
                -- User engagement level
                CASE 
                    WHEN COUNT(*) >= 10 THEN 'High Usage'
                    WHEN COUNT(*) >= 5 THEN 'Medium Usage'
                    WHEN COUNT(*) >= 1 THEN 'Low Usage'
                    ELSE 'No Usage'
                END AS usage_level
            FROM
                user_actions
            WHERE
                user_id IS NOT NULL 
                AND org_id IS NOT NULL
                AND action IS NOT NULL
                AND credit_type != 'default'
            GROUP BY
                activity_date, org_id, user_id, action, credit_type
            ORDER BY
                activity_date DESC, org_id, user_id;
        """)

        CREATE_CS_MV_INDEX = text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_cs_daily_activity_unique
            ON {view_name} (activity_date, org_id, user_id, action, credit_type);
        """)

        with engine.connect() as conn:
            conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {view_name};"))
            conn.execute(CREATE_CS_MV)
            conn.execute(CREATE_CS_MV_INDEX)
            conn.commit()

        return True
    except Exception:
        print(traceback.format_exc())
        return False
    
def create_finance_view(view_name: str) -> bool:
    try:
        engine = create_engine(DB_URL)

        # Create or replace view for easier view management
        CREATE_FINANCE_MV = text(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS
        SELECT
            org_id,
            SUM(
                CASE 
                    WHEN action = 'add' THEN credits
                    WHEN action = 'deduct' THEN -credits
                    ELSE 0
                END
            ) AS total_credits
        FROM
            user_actions
        GROUP BY
            org_id;
        """)

        CREATE_FINANCE_MV_INDEX = text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_finance_org_credit_balance_org_id
            ON {view_name} (org_id);
        """)

        with engine.connect() as conn:
            conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {view_name};"))
            conn.execute(CREATE_FINANCE_MV)
            conn.execute(CREATE_FINANCE_MV_INDEX)
            conn.commit()

        return True
    
    except Exception:
        print(traceback.format_exc())
        return False