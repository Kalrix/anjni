import os
import asyncio
import json
import requests
from dotenv import load_dotenv
from fastapi import APIRouter, Query, HTTPException
from api.app.redis_config import redis_client
from api.app.option_database import insert_option_chain  # ✅ Save live updates
from api.app.dhan_api_input import get_scrip_details  # ✅ Fetch alias directly
from api.app.option_chain import fetch_expiry_list  # ✅ Fetch expiry dynamically

# ✅ Load environment variables
load_dotenv(dotenv_path="api/app/.env")

# ✅ Fetch credentials from .env
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
CLIENT_ID = os.getenv("CLIENT_ID")

# ✅ API URLs
OPTION_CHAIN_URL = "https://api.dhan.co/v2/optionchain"

# ✅ Headers for API requests
HEADERS = {
    "access-token": ACCESS_TOKEN,
    "client-id": CLIENT_ID,
    "Content-Type": "application/json",
}

# ✅ FastAPI Router for Managing Tracked Scrips
router = APIRouter()

# ✅ Function to Fetch Live Option Chain Data
async def fetch_live_option_chain(security_id: int, exchange_segment: str, expiry: str, retries=5, delay=3):
    """Fetch real-time option chain data and push to Redis."""
    payload = {
        "UnderlyingScrip": security_id,
        "UnderlyingSeg": exchange_segment,
        "Expiry": expiry
    }

    print(f"\n📌 DEBUG: Fetching Live Data for {security_id} ({exchange_segment}) - Expiry {expiry}")
    print(json.dumps(payload, indent=4))

    for attempt in range(retries):
        try:
            response = requests.post(OPTION_CHAIN_URL, headers=HEADERS, json=payload)
            response.raise_for_status()
            option_chain_data = response.json().get("data", {})

            if not option_chain_data:
                print(f"⚠️ No live data for {security_id}-{exchange_segment} Expiry: {expiry}")
                return

            # ✅ Fetch alias from `dhan_api_input.py`
            scrip_details = get_scrip_details(security_id, "NSE", "I")
            underlying_symbol = scrip_details.get("alias", f"Scrip-{security_id}")

            print(f"✅ Using Alias as Underlying Symbol: {underlying_symbol}")

            # ✅ Cache live data in Redis (Expires in 30 sec)
            redis_key = f"live_option_chain:{security_id}:{expiry}"
            redis_client.setex(redis_key, 30, json.dumps(option_chain_data))
            print(f"✅ Live Data Cached in Redis: {redis_key}")

            # ✅ Save to TimescaleDB for historical analysis
            insert_option_chain(underlying_symbol, expiry, option_chain_data)

            # ✅ Publish data for real-time processing
            redis_client.publish("option_chain_live", json.dumps(option_chain_data))

            return  # ✅ Exit on success

        except requests.exceptions.RequestException as e:
            if response.status_code == 429:
                print(f"⚠️ Rate limit hit for {security_id}-{exchange_segment} Expiry: {expiry}, retrying in {delay} sec...")
                await asyncio.sleep(delay)
                delay *= 2  # ✅ Exponential backoff
            else:
                print(f"❌ Error fetching live option chain for {security_id}-{exchange_segment} Expiry: {expiry}: {e}")
                break

    print(f"❌ Failed to fetch option chain for {security_id}-{exchange_segment} Expiry: {expiry} after {retries} retries.")


# ✅ Function to Track Real-Time Market Data
async def track_option_chain(security_id: int, exchange_segment: str):
    """Continuously track live option chain data for the nearest & monthly expiry."""
    expiry_list = await fetch_expiry_list(security_id, exchange_segment)
    if not expiry_list:
        print(f"⚠️ No expiries found for {security_id}-{exchange_segment}, stopping tracking.")
        return

    while True:
        for expiry in expiry_list[:2]:  # ✅ Track nearest & next expiry
            await fetch_live_option_chain(security_id, exchange_segment, expiry)
            await asyncio.sleep(3)  # ✅ Poll every 3 seconds (adjust based on API limits)


# ✅ API Route to Add Security for Tracking
@router.post("/add-tracked-scrip/")
async def add_tracked_scrip(security_id: int, exchange_segment: str):
    """Add a scrip to live tracking list (Stored in Redis)."""
    redis_client.sadd("tracked_scrips", f"{security_id}:{exchange_segment}")
    print(f"✅ Added {security_id}-{exchange_segment} to live tracking")
    return {"message": f"{security_id}-{exchange_segment} added for live tracking"}


# ✅ API Route to Remove Security from Tracking
@router.post("/remove-tracked-scrip/")
async def remove_tracked_scrip(security_id: int, exchange_segment: str):
    """Remove a scrip from live tracking list (Stored in Redis)."""
    redis_client.srem("tracked_scrips", f"{security_id}:{exchange_segment}")
    print(f"✅ Removed {security_id}-{exchange_segment} from live tracking")
    return {"message": f"{security_id}-{exchange_segment} removed from live tracking"}


# ✅ Run Live Tracker for Dynamic Scrips
async def run_live_tracker():
    """Continuously track scrips added by users dynamically."""
    while True:
        tracked_scrips = redis_client.smembers("tracked_scrips")
        tasks = []

        for scrip in tracked_scrips:
            security_id, exchange_segment = scrip.decode().split(":")
            tasks.append(track_option_chain(int(security_id), exchange_segment))

        if tasks:
            await asyncio.gather(*tasks)

        await asyncio.sleep(10)  # ✅ Refresh tracked scrips every 10 seconds


if __name__ == "__main__":
    asyncio.run(run_live_tracker())
