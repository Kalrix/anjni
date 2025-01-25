from fastapi import APIRouter, HTTPException
import psycopg2
import os
import re
from dotenv import load_dotenv
from rapidfuzz import fuzz, process

# ✅ Load environment variables
load_dotenv()

# ✅ Database connection details
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

# ✅ Initialize FastAPI Router
router = APIRouter()

# ✅ Connect to PostgreSQL
def connect_db():
    """Establish a connection to the database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

# ✅ Search API Endpoint (Retaining Output Format)
@router.get("/search/")
async def search_scrip(query: str):
    """Search for a scrip based on a query and return compatible output."""
    conn = connect_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor()
    try:
        query = query.strip().upper()

        # ✅ Prioritize Exact Match First
        exact_match_sql = """
        SELECT sem_smst_security_id, COALESCE(symbol_name, 'N/A'), 
               trading_symbol, exchange, segment, attribute, enum, alias
        FROM search_table
        WHERE (symbol_name = %s OR trading_symbol = %s OR alias = %s)
        ORDER BY 
            CASE 
                WHEN exchange = 'NSE' THEN 1  -- ✅ NSE first
                WHEN exchange = 'BSE' THEN 2  -- ✅ BSE second
                ELSE 3
            END,
            CASE 
                WHEN segment = 'I' THEN 1  -- ✅ Indices First
                WHEN segment = 'E' THEN 2  -- ✅ Equities Second
                WHEN segment = 'D' THEN 3  -- ✅ Derivatives Third
                ELSE 4 
            END
        LIMIT 50;
        """
        cursor.execute(exact_match_sql, (query, query, query))
        exact_results = cursor.fetchall()

        if exact_results:
            scrips = []
            for row in exact_results:
                scrips.append({
                    "security_id": row[0],
                    "symbol_name": row[1] if row[1] else "N/A",
                    "trading_symbol": row[2] if row[2] else "N/A",
                    "exchange": row[3],
                    "segment": row[4],
                    "attribute": row[5],
                    "enum": row[6],
                    "alias": row[7],
                })
            return scrips  # ✅ Return immediately if we find exact matches

        # ✅ If No Exact Match, Use Pattern Matching
        search_sql = """
        SELECT sem_smst_security_id, COALESCE(symbol_name, 'N/A'), 
               trading_symbol, exchange, segment, attribute, enum, alias
        FROM search_table
        WHERE (symbol_name ILIKE %s OR trading_symbol ILIKE %s OR alias ILIKE %s)
        ORDER BY 
            CASE 
                WHEN exchange = 'NSE' THEN 1  -- ✅ NSE first
                WHEN exchange = 'BSE' THEN 2  -- ✅ BSE second
                ELSE 3
            END,
            CASE 
                WHEN segment = 'I' THEN 1  -- ✅ Indices First
                WHEN segment = 'E' THEN 2  -- ✅ Equities Second
                WHEN segment = 'D' THEN 3  -- ✅ Derivatives Third
                ELSE 4 
            END
        LIMIT 50;
        """
        query_param = f"%{query}%"
        cursor.execute(search_sql, (query_param, query_param, query_param))
        results = cursor.fetchall()

        scrips = []
        for row in results:
            scrips.append({
                "security_id": row[0],
                "symbol_name": row[1] if row[1] else "N/A",
                "trading_symbol": row[2] if row[2] else "N/A",
                "exchange": row[3],
                "segment": row[4],
                "attribute": row[5],
                "enum": row[6],
                "alias": row[7],
            })

        conn.close()

        return scrips  # ✅ Return pattern-matched results

    except Exception as e:
        print(f"❌ Error executing search: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        conn.close()  # ✅ Ensure DB connection is always closed
