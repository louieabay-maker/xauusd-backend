import os
import asyncio
import random
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# Initialize FastAPI App
app = FastAPI()

# Global state tracker for connected mobile apps
connected_clients = set()

# Base simulation starting price for Gold (XAUUSD)
current_sim_price = 2415.50

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    print(f"Mobile app connected to simulator. Active clients: {len(connected_clients)}")
    try:
        while True:
            # Keep connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.remove(websocket)
        print(f"Mobile app disconnected. Active clients: {len(connected_clients)}")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(stream_simulated_market_data())

async def stream_simulated_market_data():
    global current_sim_price
    print("MetaApi Bypass Active: Running live market price simulator...")
    print("Backend pipeline running smoothly on port 10000.")

    while True:
        if connected_clients:
            try:
                # Simulate realistic micro-movement ticks for Gold (random jitter between -0.15 and +0.15)
                price_fluctuation = random.uniform(-0.15, 0.15)
                current_sim_price = round(current_sim_price + price_fluctuation, 2)

                # Build the dynamic payload package
                payload = {
                    "price": current_sim_price,
                    "timestamp": asyncio.get_event_loop().time()
                }
                
                # Broadcast real-time changes to all connected frontend panels
                for client in list(connected_clients):
                    try:
                        await client.send_json(payload)
                    except Exception:
                        connected_clients.remove(client)

            except Exception as e:
                print(f"Error updating simulator stream tick: {e}")
                
        await asyncio.sleep(1)  # Streams a fresh dynamic price check every single second
