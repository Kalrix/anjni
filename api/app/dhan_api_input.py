from fastapi import APIRouter, HTTPException, Query
import psycopg2
import os
from dotenv import load_dotenv

# ✅ Load environment variables
load_dotenv(dotenv_path="api/app/.env")

# ✅ Database connection details
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

# ✅ Create FastAPI router
router = APIRouter()

# ✅ Function to establish a database connection
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

# ✅ API Route to Get Scrip Details
@router.get("/get-scrip-details/")
def get_scrip_details(
    security_id: int = Query(..., description="Security ID"),
    exchange: str = Query(..., description="Exchange"),
    segment: str = Query(..., description="Segment")
):
    """Fetch scrip details from `search_table` based on `security_id`, `exchange`, and `segment`"""
    
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
        SELECT sem_smst_security_id, attribute, enum, alias
        FROM search_table
        WHERE sem_smst_security_id = %s AND exchange = %s AND segment = %s
        LIMIT 1;
    """
    
    print(f"📌 Running Query: {query}")
    print(f"📌 Query Params: security_id={security_id}, exchange={exchange}, segment={segment}")

    cursor.execute(query, (security_id, exchange, segment))
    result = cursor.fetchone()

    cursor.close()
    conn.close()

    if not result:
        print(f"❌ Scrip {security_id} not found in search_table!")
        raise HTTPException(status_code=404, detail="Scrip not found in search_table")

    print(f"✅ Found Scrip: {result}")

    return {
        "security_id": result[0],
        "attribute": result[1],  # ✅ Exchange Segment (IDX_I, NSE_EQ, etc.)
        "enum": result[2],       # ✅ Enum ID
        "alias": result[3]       # ✅ Trading Symbol Alias
    }
