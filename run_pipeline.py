# Main script to run the data pipeline

from pipeline.extract import extract
from pipeline.transform import transform
from pipeline.load import load
from pipeline.check import check_result
from pipeline.db import test_db_connection
from pipeline.create_views import create_cs_view, create_finance_view
from pipeline.check_views import check_mat_view
from pipeline.reference_data import setup_reference_data
from pipeline.enhanced_views import create_enhanced_cs_view, create_enhanced_finance_view, create_executive_summary_view
from config import DB_URL
import traceback

try:
    # Test database connection
    test_db_connection(DB_URL)

    # setup normalized reference data tables
    print("Setting up reference data tables...")
    setup_reference_data()

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

    # create enhanced views with table joins and meaningful names
    print("\nCreating enhanced views with normalized data...")
    
    if create_enhanced_cs_view("enhanced_customer_success_view"):
        print("Successfully created Enhanced Customer Success View with org/user names")

    if create_enhanced_finance_view("enhanced_finance_view"):
        print("Successfully created Enhanced Finance View with detailed breakdowns")

    if create_executive_summary_view("executive_summary_view"):
        print("Successfully created Executive Summary View for high-level insights")

    # check the results stored in all views
    print("\nChecking original views...")
    check_mat_view("customer_success_daily_activity")
    check_mat_view("finance_org_credit_balance")
    
    print("\nChecking enhanced views...")
    check_mat_view("enhanced_customer_success_view")
    check_mat_view("enhanced_finance_view")
    check_mat_view("executive_summary_view")

except Exception:
    print(traceback.format_exc())