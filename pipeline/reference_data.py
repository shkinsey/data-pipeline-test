"""
Reference data management module for normalized database structure.

This module handles the creation and population of reference tables that provide
meaningful names and relationships for organizations and users. It replaces
cryptic IDs with human-readable information and establishes proper foreign
key relationships for data integrity.

Author: Data Pipeline Team
Version: 1.0
"""

from sqlalchemy import create_engine, text
from config import DB_URL
import traceback


def create_reference_tables() -> bool:
    """
    Create normalized reference tables for organizations and users.

    Creates two tables with proper relationships:
    - organizations: Contains org details with meaningful names and industry classifications
    - users: Contains user details with foreign key relationship to organizations

    The tables follow database normalization principles to eliminate redundancy
    and ensure data integrity through foreign key constraints.

    Returns:
        bool: True if tables created successfully, False otherwise

    Raises:
        Exception: Database connection or SQL execution errors

    Example:
        >>> if create_reference_tables():
        ...     print("Reference tables ready for data population")
    """
    try:
        engine = create_engine(DB_URL)

        with engine.connect() as conn:
            # Create organizations table - master reference for all org data
            conn.execute(
                text("""
                CREATE TABLE IF NOT EXISTS organizations (
                    org_id VARCHAR(10) PRIMARY KEY,
                    org_name VARCHAR(100) NOT NULL,
                    industry VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            )

            # Create users table with foreign key relationship to organizations
            conn.execute(
                text("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id VARCHAR(10) PRIMARY KEY,
                    user_name VARCHAR(100) NOT NULL,
                    email VARCHAR(150),
                    org_id VARCHAR(10),
                    role VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (org_id) REFERENCES organizations(org_id)
                );
            """)
            )

            conn.commit()
            print("✅ Reference tables created successfully")
            return True

    except Exception:
        print(traceback.format_exc())
        return False


def populate_organizations() -> bool:
    """
    Populate organizations table with realistic business data.

    Creates 10 organizations across diverse industries with meaningful names
    that replace the original cryptic org_000, org_001, etc. identifiers.
    Each organization represents a different industry sector for realistic
    business intelligence analysis.

    Industries covered:
    - Technology, Financial Services, Healthcare, Education, Retail
    - Manufacturing, Media, Energy, Transportation, Professional Services

    Returns:
        bool: True if population successful, False otherwise

    Raises:
        Exception: Database connection or SQL execution errors

    Note:
        This function clears existing data to ensure clean state.
        Users are deleted first to respect foreign key constraints.
    """
    try:
        engine = create_engine(DB_URL)

        # Realistic organization data across diverse industries
        org_data = [
            ("org_000", "TechCorp Solutions", "Technology"),
            ("org_001", "Global Finance Ltd", "Financial Services"),
            ("org_002", "Healthcare Partners", "Healthcare"),
            ("org_003", "EduTech Innovations", "Education"),
            ("org_004", "Retail Dynamics", "Retail"),
            ("org_005", "Manufacturing Pro", "Manufacturing"),
            ("org_006", "Media & Entertainment Co", "Media"),
            ("org_007", "Energy Solutions Inc", "Energy"),
            ("org_008", "Logistics Express", "Transportation"),
            ("org_009", "Consulting Group", "Professional Services"),
        ]

        with engine.connect() as conn:
            # Clear existing data in correct order to respect foreign key constraints
            # Must delete users first, then organizations
            conn.execute(text("DELETE FROM users;"))
            conn.execute(text("DELETE FROM organizations;"))

            # Insert organization data with meaningful names
            for org_id, org_name, industry in org_data:
                conn.execute(
                    text("""
                    INSERT INTO organizations (org_id, org_name, industry)
                    VALUES (:org_id, :org_name, :industry)
                """),
                    {"org_id": org_id, "org_name": org_name, "industry": industry},
                )

            conn.commit()
            print("✅ Organizations table populated successfully")
            return True

    except Exception:
        print(traceback.format_exc())
        return False


def populate_users() -> bool:
    """
    Populate users table with realistic employee data and proper org relationships.

    Creates 20 users (2 per organization) with realistic names, email addresses,
    and role assignments. Each user is properly associated with an organization
    through foreign key relationships, ensuring data integrity.

    User distribution:
    - 2 users per organization (20 total)
    - Realistic names and email addresses
    - Diverse roles: Admin, Developer, Manager, Analyst, etc.
    - Email domains match organization names for authenticity

    Returns:
        bool: True if population successful, False otherwise

    Raises:
        Exception: Database connection or SQL execution errors

    Note:
        Organizations must be populated first due to foreign key constraints.
        Users table is cleared but organizations remain intact.
    """
    try:
        engine = create_engine(DB_URL)

        # Realistic user data with proper org assignments and diverse roles
        user_data = [
            # TechCorp Solutions (org_000)
            (
                "user_000",
                "Alice Johnson",
                "alice.johnson@techcorp.com",
                "org_000",
                "Admin",
            ),
            ("user_001", "Bob Smith", "bob.smith@techcorp.com", "org_000", "Developer"),
            # Global Finance Ltd (org_001)
            (
                "user_002",
                "Carol Davis",
                "carol.davis@globalfinance.com",
                "org_001",
                "Analyst",
            ),
            (
                "user_003",
                "David Wilson",
                "david.wilson@globalfinance.com",
                "org_001",
                "Manager",
            ),
            # Healthcare Partners (org_002)
            (
                "user_004",
                "Emma Brown",
                "emma.brown@healthcare.com",
                "org_002",
                "Coordinator",
            ),
            (
                "user_005",
                "Frank Miller",
                "frank.miller@healthcare.com",
                "org_002",
                "Specialist",
            ),
            # EduTech Innovations (org_003)
            ("user_006", "Grace Lee", "grace.lee@edutech.com", "org_003", "Teacher"),
            ("user_007", "Henry Clark", "henry.clark@edutech.com", "org_003", "Admin"),
            # Retail Dynamics (org_004)
            (
                "user_008",
                "Ivy Martinez",
                "ivy.martinez@retail.com",
                "org_004",
                "Sales Rep",
            ),
            ("user_009", "Jack Taylor", "jack.taylor@retail.com", "org_004", "Manager"),
            # Manufacturing Pro (org_005)
            (
                "user_010",
                "Kate Anderson",
                "kate.anderson@manufacturing.com",
                "org_005",
                "Engineer",
            ),
            (
                "user_011",
                "Liam Thompson",
                "liam.thompson@manufacturing.com",
                "org_005",
                "Supervisor",
            ),
            # Media & Entertainment Co (org_006)
            ("user_012", "Mia Garcia", "mia.garcia@media.com", "org_006", "Producer"),
            (
                "user_013",
                "Noah Rodriguez",
                "noah.rodriguez@media.com",
                "org_006",
                "Editor",
            ),
            # Energy Solutions Inc (org_007)
            (
                "user_014",
                "Olivia Hernandez",
                "olivia.hernandez@energy.com",
                "org_007",
                "Technician",
            ),
            ("user_015", "Paul Lopez", "paul.lopez@energy.com", "org_007", "Manager"),
            # Logistics Express (org_008)
            (
                "user_016",
                "Quinn Gonzalez",
                "quinn.gonzalez@logistics.com",
                "org_008",
                "Driver",
            ),
            (
                "user_017",
                "Rachel Perez",
                "rachel.perez@logistics.com",
                "org_008",
                "Dispatcher",
            ),
            # Consulting Group (org_009)
            (
                "user_018",
                "Sam Turner",
                "sam.turner@consulting.com",
                "org_009",
                "Consultant",
            ),
            (
                "user_019",
                "Tina Phillips",
                "tina.phillips@consulting.com",
                "org_009",
                "Senior Consultant",
            ),
        ]

        with engine.connect() as conn:
            # Clear existing user data (organizations already cleared in populate_organizations)
            conn.execute(text("DELETE FROM users;"))

            # Insert user data with proper foreign key relationships
            for user_id, user_name, email, org_id, role in user_data:
                conn.execute(
                    text("""
                    INSERT INTO users (user_id, user_name, email, org_id, role)
                    VALUES (:user_id, :user_name, :email, :org_id, :role)
                """),
                    {
                        "user_id": user_id,
                        "user_name": user_name,
                        "email": email,
                        "org_id": org_id,
                        "role": role,
                    },
                )

            conn.commit()
            print("✅ Users table populated successfully")
            return True

    except Exception:
        print(traceback.format_exc())
        return False


def setup_reference_data() -> bool:
    """
    Complete setup orchestration for all reference data tables.

    This is the main entry point for setting up the normalized database
    structure. It coordinates the creation and population of both reference
    tables in the correct order to respect dependencies.

    Execution order:
    1. Create reference tables (organizations, users)
    2. Populate organizations table
    3. Populate users table (depends on organizations)

    Returns:
        bool: True if complete setup successful, False otherwise

    Example:
        >>> if setup_reference_data():
        ...     print("Normalized database structure ready")
        ...     print("Enhanced views can now use meaningful names")
    """
    if create_reference_tables():
        if populate_organizations():
            if populate_users():
                print("✅ All reference data setup completed successfully")
                return True
    return False
