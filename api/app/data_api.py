from fastapi import APIRouter, Query
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path="api/app/.env")

# Database connection details
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

# ✅ Use APIRouter to properly register routes
router = APIRouter()

def get_db_connection():
    """Creates and returns a database connection."""
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

@router.get("/get-data/")
def get_data(
    instrument: str = Query(None, description="Filter by instrument"),
    exchange: str = Query(None, description="Filter by exchange")
):
    """Fetch filtered data from the database."""
    conn = get_db_connection()
    cursor = conn.cursor()

    query = "SELECT SEM_EXM_EXCH_ID, SEM_INSTRUMENT_NAME, SEM_TRADING_SYMBOL, SEM_EXPIRY_DATE, fetch_timestamp FROM scrip_master"
    conditions = []
    params = []

    if instrument:
        conditions.append("SEM_INSTRUMENT_NAME = %s")
        params.append(instrument)

    if exchange:
        conditions.append("SEM_EXM_EXCH_ID = %s")
        params.append(exchange)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY fetch_timestamp DESC LIMIT 50;"

    cursor.execute(query, params)
    data = cursor.fetchall()

    cursor.close()
    conn.close()

    response = [
        {
            "exchange": row[0],
            "instrument": row[1],
            "trading_symbol": row[2],
            "expiry_date": row[3],
            "timestamp": row[4]
        }
        for row in data
    ]

    return {"data": response}
