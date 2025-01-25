import os
import asyncio
import requests
import json
from dotenv import load_dotenv
from fastapi import APIRouter, Query, HTTPException
from api.app.redis_config import redis_client
from api.app.option_database import insert_option_chain  # ✅ Importing DB insert function
from api.app.dhan_api_input import get_scrip_details  # ✅ Fetch alias directly

# ✅ Load environment variables
load_dotenv(dotenv_path="api/app/.env")

# ✅ Fetch credentials from .env
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
CLIENT_ID = os.getenv("CLIENT_ID")

# ✅ FastAPI Router
router = APIRouter()

# ✅ API URLs
OPTION_CHAIN_URL = "https://api.dhan.co/v2/optionchain"
EXPIRY_LIST_URL = "https://api.dhan.co/v2/optionchain/expirylist"

# ✅ Headers for API requests
HEADERS = {
    "access-token": ACCESS_TOKEN,
    "client-id": CLIENT_ID,
    "Content-Type": "application/json",
}

# ✅ Segment Mapping (Fixes Incorrect Querying)
SEGMENT_MAPPING = {
    "NSE_EQ": "E",
    "BSE_EQ": "E",
    "IDX_I": "I",
    "FUTIDX": "D",
    "OPTIDX": "D",
    "FUTSTK": "D",
    "OPTSTK": "D",
}

# ✅ Fetch Expiry List
async def fetch_expiry_list(security_id: int, exchange_segment: str):
    """Retrieve all expiry dates for a given instrument."""
    payload = {"UnderlyingScrip": security_id, "UnderlyingSeg": exchange_segment}

    # 🔹 Print Payload for Debugging
    print(f"\n📌 Fetch Expiry List Payload:\n{json.dumps(payload, indent=4)}\n")

    try:
        response = requests.post(EXPIRY_LIST_URL, headers=HEADERS, json=payload)
        response.raise_for_status()
        expiry_list = response.json().get("data", [])

        if not expiry_list:
            print(f"⚠️ No expiries received for {security_id}-{exchange_segment}")
            return []

        print(f"✅ Expiry List Fetched for {security_id}-{exchange_segment}: {expiry_list}")
        return expiry_list
    except requests.RequestException as e:
        print(f"❌ Error fetching expiry list for {security_id}-{exchange_segment}: {e}")
        return []

# ✅ Select Expiries to Prioritize
def select_relevant_expiries(expiry_list):
    """Select nearest expiry + monthly expiry for reduced API calls."""
    expiry_list = sorted(expiry_list)  # Sort to get nearest expiry first
    nearest_expiry = expiry_list[0] if expiry_list else None
    monthly_expiry = None

    for expiry in expiry_list:
        if expiry[-2:] == "27":  # ✅ Assuming monthly expiry ends in "27"
            monthly_expiry = expiry
            break

    selected_expiries = list(filter(None, [nearest_expiry, monthly_expiry]))  # ✅ Only return valid expiries

    # 🔹 Print Selected Expiries
    print(f"\n📌 Selected Expiries for Option Chain Fetch:\n{selected_expiries}\n")

    return selected_expiries

# ✅ Fetch Option Chain Data for One Expiry
async def fetch_option_chain(security_id: int, exchange_segment: str, expiry: str, retries=5, delay=5):
    """Retrieve Option Chain Data for a given expiry with retry logic on 429 errors."""
    payload = {
        "UnderlyingScrip": security_id,
        "UnderlyingSeg": exchange_segment,
        "Expiry": expiry  # ✅ Single expiry request
    }

    # 🔹 Debugging API Calls
    print(f"\n📌 DEBUG: Sending Option Chain API Request...\n"
          f"🔹 Endpoint: {OPTION_CHAIN_URL}\n"
          f"🔹 Headers: {HEADERS}\n"
          f"🔹 Payload:\n{json.dumps(payload, indent=4)}\n")

    for attempt in range(retries):
        try:
            response = requests.post(OPTION_CHAIN_URL, headers=HEADERS, json=payload)
            response.raise_for_status()
            option_chain_data = response.json().get("data", {})

            if not option_chain_data:
                print(f"⚠️ No option chain data received for {security_id}-{exchange_segment} Expiry: {expiry}")
                return {}

            # ✅ Corrected segment before fetching alias
            corrected_segment = SEGMENT_MAPPING.get(exchange_segment, "E")

            # ✅ Get alias directly from dhan_api_input.py
            scrip_details = get_scrip_details(security_id, "NSE", corrected_segment)
            underlying_symbol = scrip_details.get("alias", f"Scrip-{security_id}")

            print(f"✅ Using Alias as Underlying Symbol: {underlying_symbol}")

            # 🔹 Print Response Structure for Debugging
            print(f"\n📌 Option Chain Response Structure ({expiry}):\n{json.dumps(option_chain_data, indent=4)}\n")

            # ✅ Cache expiry data in Redis (5 minutes)
            redis_key = f"option_chain:{security_id}:{expiry}"
            redis_client.setex(redis_key, 300, json.dumps(option_chain_data))
            print(f"✅ Option Chain Data Cached: {redis_key}")

            # ✅ Save to TimescaleDB
            insert_option_chain(underlying_symbol, expiry, option_chain_data)

            return option_chain_data

        except requests.exceptions.RequestException as e:
            if response.status_code == 429:
                print(f"⚠️ Rate limit hit for {security_id}-{exchange_segment} Expiry: {expiry}, retrying in {delay} sec...")
                await asyncio.sleep(delay)
                delay *= 2  # ✅ Exponential backoff
            else:
                print(f"❌ Error fetching option chain for {security_id}-{exchange_segment} Expiry: {expiry}: {e}")
                break

    print(f"❌ Failed to fetch option chain for {security_id}-{exchange_segment} Expiry: {expiry} after {retries} retries.")
    return {}

# ✅ API Route to Fetch Option Chain Data
@router.get("/get_option_chain/")
async def get_option_chain(
    security_id: int = Query(..., description="Security ID of the instrument"),
    exchange_segment: str = Query(..., description="Exchange segment of the instrument"),
):
    """Fetch option chain data for the nearest expiry and next monthly expiry."""

    expiry_list = await fetch_expiry_list(security_id, exchange_segment)
    if not expiry_list:
        raise HTTPException(status_code=404, detail="No expiry dates found for given scrip")

    selected_expiries = select_relevant_expiries(expiry_list)
    if not selected_expiries:
        raise HTTPException(status_code=500, detail="No valid expiries found.")

    option_chain_results = {}

    for expiry in selected_expiries:
        option_chain_data = await fetch_option_chain(security_id, exchange_segment, expiry)
        if option_chain_data:
            option_chain_results[expiry] = option_chain_data
        await asyncio.sleep(3)  # ✅ Wait 3 sec before next API call (to avoid rate limits)

    if not option_chain_results:
        raise HTTPException(status_code=500, detail="Failed to fetch option chain data.")

    # 🔹 Print Final Response Structure
    print(f"\n📌 Final Response Structure:\n{json.dumps(option_chain_results, indent=4)}\n")

    return {
        "security_id": security_id,
        "exchange_segment": exchange_segment,
        "option_chain": option_chain_results
    }
