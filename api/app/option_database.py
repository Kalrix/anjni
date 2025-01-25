import psycopg2
import os
import json
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException
from datetime import datetime

# ✅ Load environment variables
load_dotenv(dotenv_path="api/app/.env")

# ✅ Fetch database credentials
DB_CONN = os.getenv("DATABASE_URL")

# ✅ Initialize FastAPI Router
router = APIRouter()

# ✅ Establish a connection function
def get_db_connection():
    """Create and return a database connection."""
    try:
        conn = psycopg2.connect(DB_CONN)
        return conn
    except Exception as e:
        print(f"❌ Database Connection Error: {e}")
        return None

# ✅ Function to insert option chain data into TimescaleDB
def insert_option_chain(underlying, expiry, option_chain_data):
    """Insert option chain data into TimescaleDB with batch processing."""
    if not option_chain_data:
        print(f"⚠️ No data to insert for {underlying} - {expiry}")
        return

    conn = get_db_connection()
    if not conn:
        print("❌ Skipping database insertion due to connection failure.")
        return

    cursor = conn.cursor()

    print(f"🔄 Inserting data for {underlying} - Expiry {expiry}")

    insert_query = """
        INSERT INTO option_data 
        (timestamp, underlying, expiry, strike, ce_oi, pe_oi, ce_iv, pe_iv, ce_price, pe_price, 
         ce_delta, pe_delta, ce_theta, pe_theta, ce_gamma, pe_gamma, ce_vega, pe_vega, 
         ce_top_ask_price, pe_top_ask_price, ce_top_ask_quantity, pe_top_ask_quantity, 
         ce_top_bid_price, pe_top_bid_price, ce_top_bid_quantity, pe_top_bid_quantity, 
         ce_previous_close_price, pe_previous_close_price, ce_previous_oi, pe_previous_oi, 
         ce_previous_volume, pe_previous_volume, volume)
        VALUES (now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s)
    """

    batch_data = []

    for strike, data in option_chain_data.get("oc", {}).items():
        ce = data.get("ce", {})
        pe = data.get("pe", {})

        batch_data.append((
            underlying, expiry, float(strike),
            ce.get("oi", 0), pe.get("oi", 0),
            ce.get("implied_volatility", 0), pe.get("implied_volatility", 0),
            ce.get("last_price", 0), pe.get("last_price", 0),
            ce.get("greeks", {}).get("delta", 0), pe.get("greeks", {}).get("delta", 0),
            ce.get("greeks", {}).get("theta", 0), pe.get("greeks", {}).get("theta", 0),
            ce.get("greeks", {}).get("gamma", 0), pe.get("greeks", {}).get("gamma", 0),
            ce.get("greeks", {}).get("vega", 0), pe.get("greeks", {}).get("vega", 0),
            ce.get("top_ask_price", 0), pe.get("top_ask_price", 0),
            ce.get("top_ask_quantity", 0), pe.get("top_ask_quantity", 0),
            ce.get("top_bid_price", 0), pe.get("top_bid_price", 0),
            ce.get("top_bid_quantity", 0), pe.get("top_bid_quantity", 0),
            ce.get("previous_close_price", 0), pe.get("previous_close_price", 0),
            ce.get("previous_oi", 0), pe.get("previous_oi", 0),
            ce.get("previous_volume", 0), pe.get("previous_volume", 0),
            ce.get("volume", 0) + pe.get("volume", 0)
        ))

    try:
        cursor.executemany(insert_query, batch_data)  # ✅ Batch insert for efficiency
        conn.commit()
        print(f"✅ Successfully inserted {len(batch_data)} strikes for expiry {expiry} ({underlying})")

    except Exception as e:
        conn.rollback()  # ✅ Rollback in case of failure
        print(f"❌ Database Insertion Error: {e}")

    finally:
        cursor.close()
        conn.close()

# ✅ FastAPI Endpoint to Save Option Chain Data
@router.post("/save_option_chain/")
async def save_option_chain(
    security_id: int, exchange_segment: str, expiry: str, option_chain_data: dict
):
    """Save option chain data for a given security ID, exchange segment, and expiry."""
    if not option_chain_data:
        raise HTTPException(status_code=400, detail="Invalid option chain data")

    # ✅ Fetch Underlying Name from Security ID
    underlying = get_underlying_symbol(security_id)
    if not underlying:
        raise HTTPException(status_code=404, detail=f"Underlying symbol not found for security ID {security_id}")

    insert_option_chain(underlying, expiry, option_chain_data)
    return {"message": f"✅ Option chain data saved for {underlying} - Expiry {expiry}"}

# ✅ Function to Get Underlying Symbol from Database
def get_underlying_symbol(security_id: int):
    """Fetch the underlying symbol for a given security ID."""
    conn = get_db_connection()
    if not conn:
        return None

    cursor = conn.cursor()
    query = """
        SELECT alias FROM search_table WHERE sem_smst_security_id = %s LIMIT 1;
    """
    cursor.execute(query, (security_id,))
    result = cursor.fetchone()

    cursor.close()
    conn.close()

    return result[0] if result else None
