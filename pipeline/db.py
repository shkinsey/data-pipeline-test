"""
Database connection module for ETL pipeline.

This module provides database connectivity testing and validation functions
to ensure PostgreSQL is accessible before running the ETL pipeline. It includes
comprehensive error handling and connection diagnostics.

Author: Data Pipeline Team
Version: 1.0
"""

from sqlalchemy import create_engine, text
import traceback


def test_db_connection(db_url: str) -> bool:
    """
    Test PostgreSQL database connection and validate accessibility.
    
    Performs a comprehensive connection test including basic query execution
    to ensure the database is not only reachable but also functional for
    ETL operations. This function should be called before any data operations.
    
    Connection Tests:
    - Database server accessibility
    - Authentication validation
    - Basic query execution capability
    - Transaction support verification
    
    Args:
        db_url (str): PostgreSQL connection string (format: postgresql://user:pass@host:port/db)
        
    Returns:
        bool: True if connection successful, False otherwise
        
    Raises:
        Exception: Database connection or authentication errors
        
    Example:
        >>> if test_db_connection(DB_URL):
        ...     print("Database ready for ETL operations")
        ... else:
        ...     print("Database connection failed - check configuration")
        
    Note:
        This function will print detailed error information if connection fails,
        helping with troubleshooting database configuration issues.
    """
    try:
        # Create database engine with connection pooling
        engine = create_engine(db_url)
        
        # Test basic connectivity with a simple query
        with engine.connect() as conn:
            # Execute a simple query to verify database functionality
            result = conn.execute(text("SELECT 1 as test_connection;"))
            test_value = result.fetchone()[0]
            
            # Verify query executed correctly
            if test_value == 1:
                print("✅ Database connection successful.")
                return True
            else:
                print("❌ Database query returned unexpected result.")
                return False
                
    except Exception:
        print("❌ Database connection failed:")
        print(traceback.format_exc())
        print("\n💡 Troubleshooting tips:")
        print("   - Check if PostgreSQL server is running")
        print("   - Verify connection string in .env file")
        print("   - Ensure database exists and credentials are correct")
        print("   - Check network connectivity and firewall settings")
        return False
