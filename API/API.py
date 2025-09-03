from fastapi import FastAPI, HTTPException, Depends, status, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Optional, Dict, Any
from datetime import datetime, date
import logging
from config import DB_URL

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app with metadata
app = FastAPI(
    title="Data Pipeline Analytics API",
    description="REST API for accessing finance and customer success analytics from the data pipeline",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security
security = HTTPBearer()

# Database connection
def get_db_engine():
    """Get database engine with connection pooling"""
    try:
        engine = create_engine(
            DB_URL,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=300
        )
        return engine
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection failed"
        )

# Pydantic Models
class OrganizationFinance(BaseModel):
    organization: str
    industry: str
    org_id: str
    net_credit_balance: float
    total_credits_added: float
    total_credits_used: float
    active_users: int
    total_transactions: int
    invoice_status: str
    avg_transaction_value: float

class CustomerSuccessUser(BaseModel):
    organization: str
    industry: str
    user_name: str
    user_role: str
    user_email: str
    first_activity_date: date
    last_activity_date: date
    active_days: int
    net_credit_balance: float
    total_credits_purchased: float
    total_credits_consumed: float
    total_actions: int
    purchase_actions: int
    usage_actions: int
    engagement_level: str
    customer_status: str
    avg_daily_usage: Optional[float]

class ExecutiveSummary(BaseModel):
    organization: str
    industry: str
    total_users: int
    active_days: int
    total_credit_volume: float
    total_actions: int
    avg_credit_per_action: float
    primary_action_type: str
    most_active_user: str
    first_activity: date
    last_activity: date

class APIResponse(BaseModel):
    success: bool
    data: Optional[Any] = None
    message: str
    timestamp: datetime = Field(default_factory=datetime.now)
    count: Optional[int] = None

class ErrorResponse(BaseModel):
    success: bool = False
    error: str
    detail: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.now)

# Authentication dependency
async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    Simple token verification - replace with proper JWT validation in production
    For demo purposes, accepts any token starting with 'demo-'
    """
    if not credentials.credentials.startswith('demo-'):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return credentials.credentials

# Health check endpoint
@app.get("/health", response_model=Dict[str, str])
async def health_check():
    """Health check endpoint to verify API and database connectivity"""
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service unhealthy"
        )

@app.get("/", response_model=Dict[str, str])
def read_root():
    """Root endpoint with API information"""
    return {
        "message": "Data Pipeline Analytics API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }

# Finance Team Endpoints
@app.get("/api/v1/finance/organizations", 
         response_model=APIResponse,
         dependencies=[Depends(verify_token)],
         tags=["Finance"])
async def get_finance_data(
    limit: Optional[int] = Query(None, ge=1, le=100, description="Limit number of results"),
    offset: Optional[int] = Query(0, ge=0, description="Offset for pagination"),
    invoice_status: Optional[str] = Query(None, description="Filter by invoice status")
):
    """
    Get financial data for all organizations including credit balances and invoice status
    
    - **limit**: Maximum number of organizations to return (1-100)
    - **offset**: Number of records to skip for pagination
    - **invoice_status**: Filter by specific invoice status
    """
    try:
        engine = get_db_engine()
        
        # Build query with optional filters
        query = "SELECT * FROM enhanced_finance_view"
        conditions = []
        
        if invoice_status:
            conditions.append(f"invoice_status = '{invoice_status}'")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY net_credit_balance DESC"
        
        if limit:
            query += f" LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            data = [OrganizationFinance(**row._asdict()) for row in result]
            
        return APIResponse(
            success=True,
            data=data,
            message="Finance data retrieved successfully",
            count=len(data)
        )
        
    except SQLAlchemyError as e:
        logger.error(f"Database error in finance endpoint: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database query failed"
        )
    except Exception as e:
        logger.error(f"Unexpected error in finance endpoint: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@app.get("/api/v1/finance/organizations/{org_id}",
         response_model=APIResponse,
         dependencies=[Depends(verify_token)],
         tags=["Finance"])
async def get_organization_finance(org_id: str):
    """Get detailed financial information for a specific organization"""
    try:
        engine = get_db_engine()
        
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT * FROM enhanced_finance_view WHERE org_id = :org_id"),
                {"org_id": org_id}
            )
            row = result.fetchone()
            
            if not row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Organization {org_id} not found"
                )
            
            data = OrganizationFinance(**row._asdict())
            
        return APIResponse(
            success=True,
            data=data,
            message=f"Finance data for {org_id} retrieved successfully"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving finance data for {org_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve organization data"
        )

# Customer Success Team Endpoints
@app.get("/api/v1/customer-success/users",
         response_model=APIResponse,
         dependencies=[Depends(verify_token)],
         tags=["Customer Success"])
async def get_customer_success_data(
    limit: Optional[int] = Query(None, ge=1, le=100),
    offset: Optional[int] = Query(0, ge=0),
    engagement_level: Optional[str] = Query(None, description="Filter by engagement level"),
    customer_status: Optional[str] = Query(None, description="Filter by customer status"),
    organization: Optional[str] = Query(None, description="Filter by organization name"),
    user: Optional[str] = Query(None, description="Filter by user name")
):
    """
    Get customer success analytics for all users
    
    - **engagement_level**: Power User, Active User, Regular User, Light User
    - **customer_status**: Healthy, At Risk - Inactive, Not Using Credits, Low Balance - Upsell Opportunity
    """
    try:
        engine = get_db_engine()
        
        query = "SELECT * FROM enhanced_customer_success_view"
        conditions = []
        
        if engagement_level:
            conditions.append(f"engagement_level = '{engagement_level}'")
        if customer_status:
            conditions.append(f"customer_status = '{customer_status}'")
        if organization:
            conditions.append(f"organization = '{organization}'")
        if user:
            conditions.append(f"user_name = '{user}'")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY total_actions DESC"
        
        if limit:
            query += f" LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            data = [CustomerSuccessUser(**row._asdict()) for row in result]
            
        return APIResponse(
            success=True,
            data=data,
            message="Customer success data retrieved successfully",
            count=len(data)
        )
        
    except Exception as e:
        logger.error(f"Error in customer success endpoint: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve customer success data"
        )

# Executive Dashboard Endpoints
@app.get("/api/v1/executive/summary",
         response_model=APIResponse,
         dependencies=[Depends(verify_token)],
         tags=["Executive"])
async def get_executive_summary(
    limit: Optional[int] = Query(None, ge=1, le=50),
    industry: Optional[str] = Query(None, description="Filter by industry")
):
    """Get high-level executive summary across all organizations"""
    try:
        engine = get_db_engine()
        
        query = "SELECT * FROM executive_summary_view"
        
        if industry:
            query += f" WHERE industry = '{industry}'"
        
        query += " ORDER BY total_credit_volume DESC"
        
        if limit:
            query += f" LIMIT {limit}"
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            data = [ExecutiveSummary(**row._asdict()) for row in result]
            
        return APIResponse(
            success=True,
            data=data,
            message="Executive summary retrieved successfully",
            count=len(data)
        )
        
    except Exception as e:
        logger.error(f"Error in executive summary endpoint: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve executive summary"
        )

# Analytics Endpoints
@app.get("/api/v1/analytics/metrics",
         response_model=APIResponse,
         dependencies=[Depends(verify_token)],
         tags=["Analytics"])
async def get_platform_metrics():
    """Get overall platform metrics and KPIs"""
    try:
        engine = get_db_engine()
        
        with engine.connect() as conn:
            # Get aggregate metrics
            metrics_query = text("""
                SELECT 
                    COUNT(DISTINCT organization) as total_organizations,
                    SUM(total_users) as total_users,
                    SUM(total_credit_volume) as platform_credit_volume,
                    AVG(avg_credit_per_action) as platform_avg_credit_per_action,
                    COUNT(*) as active_organizations
                FROM executive_summary_view
            """)
            
            result = conn.execute(metrics_query)
            metrics = result.fetchone()._asdict()
            
            # Get industry breakdown
            industry_query = text("""
                SELECT 
                    industry,
                    COUNT(*) as org_count,
                    SUM(total_credit_volume) as industry_volume
                FROM executive_summary_view
                GROUP BY industry
                ORDER BY industry_volume DESC
            """)
            
            result = conn.execute(industry_query)
            industry_breakdown = [dict(row._asdict()) for row in result]
            
            data = {
                "platform_metrics": metrics,
                "industry_breakdown": industry_breakdown
            }
            
        return APIResponse(
            success=True,
            data=data,
            message="Platform analytics retrieved successfully"
        )
        
    except Exception as e:
        logger.error(f"Error in analytics endpoint: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve platform analytics"
        )

# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        content=ErrorResponse(
            error=exc.detail,
            detail=f"HTTP {exc.status_code}"
        ).model_dump(),
        status_code=exc.status_code
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        content=ErrorResponse(
            error="Internal server error",
            detail="An unexpected error occurred"
        ).model_dump(),
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000,
        log_level="info",
    )
