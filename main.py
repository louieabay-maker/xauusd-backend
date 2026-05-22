import os
import asyncio
import random
import uvicorn
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from starlette.requests import Request

app = FastAPI(title="XAUUSD Advanced Engine v3", version="3.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
                pass

manager = ConnectionManager()

# Global state to keep track of chart history and signal blocks
chart_history = []
current_price = 2422.37
ema9 = 2349.98
ema21 = 2348.73

# Initialize history with 20 base data points so the chart doesn't look blank on load
for i in range(20):
    time_str = datetime.now().strftime("%H:%M:%S")
    chart_history.append({"time": time_str, "price": current_price + random.uniform(-2, 2)})

async def generate_market_data():
    global current_price, ema9, ema21, chart_history
    
    while True:
        # Move price naturally around your requested chart thresholds
        price_change = round(random.uniform(-0.60, 0.60), 2)
        current_price = round(current_price + price_change, 2)
        
        # Keep tracking minor variations of your indicators
        ema9 = round(ema9 + (price_change * 0.1), 2)
        ema21 = round(ema21 + (price_change * 0.05), 2)
        rsi = round(random.uniform(65.0, 89.0), 1)  # Matching your high RSI screen setup
        
        time_str = datetime.now().strftime("%H:%M:%S")
        
        # Append new point and limit history window size to 30 items
        chart_history.append({"time": time_str, "price": current_price})
        if len(chart_history) > 30:
            chart_history.pop(0)
            
        payload = {
            "symbol": "XAUUSD",
            "price": current_price,
            "change": price_change,
            "rsi": rsi,
            "ema9": ema9,
            "ema21": ema21,
            "prediction": "BUY Prediction",
            "reason": "[30m] Bullish breakout. FVG detected",
            "tp": round(current_price + 12.0, 2),
            "sl": round(current_price - 8.5, 2),
            "chartData": chart_history
        }
        
        await manager.broadcast(payload)
        await asyncio.sleep(1.0) # Tick updates out instantly every single second

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(generate_market_data())

@app.api_route("/", methods=["GET", "HEAD"])
async def root_endpoint(request: Request):
    return {"status": "synchronized", "engine": "XAUUSD Broadcast Pipeline v3"}

@app.websocket("/ws")
async def websocket_stream_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Send initial historical state to the app the exact millisecond it hooks up
        await websocket.send_json({"type": "init", "chartData": chart_history})
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
