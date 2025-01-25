import json
import asyncio
import websockets
import redis
from api.app.redis_config import redis_client

# ✅ WebSocket Clients
clients = set()

# ✅ Function to Broadcast Live Data
async def broadcast_live_data():
    """Listen to Redis Pub/Sub and push live data to WebSocket clients."""
    pubsub = redis_client.pubsub()
    pubsub.subscribe("option_chain_live")

    async for message in pubsub.listen():
        if message["type"] == "message":
            data = message["data"].decode("utf-8")
            if clients:
                await asyncio.gather(*[client.send(data) for client in clients])

# ✅ WebSocket Connection Handler
async def websocket_handler(websocket, path):
    """Handle new WebSocket connections."""
    clients.add(websocket)
    try:
        async for message in websocket:
            pass
    except:
        pass
    finally:
        clients.remove(websocket)

# ✅ Run WebSocket Server
async def run_websocket_server():
    """Start WebSocket server for real-time streaming."""
    server = await websockets.serve(websocket_handler, "0.0.0.0", 8765)
    print("✅ WebSocket Server Running on ws://0.0.0.0:8765")
    await asyncio.gather(server.wait_closed(), broadcast_live_data())

if __name__ == "__main__":
    asyncio.run(run_websocket_server())
