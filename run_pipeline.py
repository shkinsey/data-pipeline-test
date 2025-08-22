# Main script to run the data pipeline

from pipeline.extract import extract
from pipeline.transform import transform
from pipeline.load import load
from pipeline.check import check_result
from pipeline.db import test_db_connection
from pipeline.create_views import create_cs_view, create_finance_view
from pipeline.check_views import check_mat_view
from config import DB_URL
import traceback

try:
    # Test database connection
    test_db_connection(DB_URL)

    # Run the data pipeline
    df = extract("data/test_data.csv")
    transformed = transform(df)
    load(transformed, "user_actions")

    # Check the ETL result in the database 
    print("Checking table")
    check_result("user_actions")

    # create materialised views for customer success and finance teams
    if create_cs_view("customer_success_daily_activity"):
        print("Successfully created/replaced Customer Success View")

    if create_finance_view("finance_org_credit_balance"):
        print("Successfully created/replaced Finance View")

    # check the results stored in the new db schema
    check_mat_view("customer_success_daily_activity")
    check_mat_view("finance_org_credit_balance")

except Exception:
    print(traceback.format_exc())