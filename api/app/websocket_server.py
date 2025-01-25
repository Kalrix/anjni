import asyncio
import json
import redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# ✅ Initialize FastAPI app
app = FastAPI()

# ✅ Enable CORS for WebSockets
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ✅ Allow frontend requests
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Redis Client
redis_client = redis.Redis(host="localhost", port=6379, db=0)

# ✅ Track connected WebSockets
active_connections = set()

# ✅ WebSocket Route
@app.websocket("/ws/option_chain/{security_id}/{expiry}")
async def websocket_endpoint(websocket: WebSocket, security_id: int, expiry: str):
    """Handles WebSocket connections for live option chain updates."""
    await websocket.accept()
    active_connections.add(websocket)
    print(f"✅ WebSocket Connected: {websocket.client}")

    try:
        while True:
            # ✅ Fetch latest option chain from Redis
            redis_key = f"live_option_chain:{security_id}:{expiry}"
            data = redis_client.get(redis_key)

            if data:
                option_chain_data = json.loads(data)
                await websocket.send_json(option_chain_data)
            else:
                await websocket.send_json({"message": "No live data available"})

            await asyncio.sleep(2)  # ✅ Poll every 2 seconds

    except WebSocketDisconnect:
        print(f"⚠️ WebSocket Disconnected: {websocket.client}")
        active_connections.remove(websocket)

