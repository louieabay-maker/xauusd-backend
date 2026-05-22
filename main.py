import os
import asyncio
import random
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import Response

app = FastAPI(title="XAUUSD GoldPulse Stream Engine", version="2.0.0")

# Configure robust CORS handling for Base44 domains
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Active streaming connections registry
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                # Handle stale or dropped client sockets gracefully
                pass

manager = ConnectionManager()

# Background price broadcasting simulator (Direct Live-Feed Layer)
async def track_gold_market_feed():
    """
    Simulates high-precision, real-time XAUUSD pricing tick-by-tick.
    Replace the random generator logic with your direct price feed API provider here.
    """
    current_price = 2420.50
    while True:
        # Generate micro-scalping price ticks
        price_change = round(random.uniform(-0.35, 0.35), 2)
        current_price = round(current_price + price_change, 2)
        
        feed_payload = {
            "symbol": "XAUUSD",
            "price": current_price,
            "change": price_change,
            "status": "live_streaming",
            "timestamp": asyncio.get_event_loop().time()
        }
        
        await manager.broadcast(feed_payload)
        await asyncio.sleep(1.0)  # Streams an updated price tick every single second

@app.on_event("startup")
async def startup_event():
    # Spin up the direct market tracking task automatically on boot
    asyncio.create_task(track_gold_market_feed())

# Dual-route root endpoint to clear Render health checker warnings
@app.api_route("/", methods=["GET", "HEAD"])
async def root_endpoint(request: Request):
    return {
        "status": "synchronized",
        "engine": "XAUUSD Direct Stream Pipeline v2",
        "environment": "Render Production"
    }

# Dedicated Direct WebSocket Route
@app.websocket("/ws")
async def websocket_stream_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Keep the socket pipeline open and listening for incoming client requests
        while True:
            client_heartbeat = await websocket.receive_text()
            # Send back immediate ping acknowledgement if requested
            await websocket.send_json({"heartbeat": "acknowledged"})
    except WebSocketDisconnect:
        manager.disconnect(websocket)
