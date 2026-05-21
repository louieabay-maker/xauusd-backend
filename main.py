import os
import asyncio
import numpy as np
import pandas as pd
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from metaapi_cloud_sdk import MetaApi

# Initialize FastAPI App
app = FastAPI()

# Retrieve Environment Variables from Render Config
token = os.getenv("METAAPI_TOKEN")
account_id = os.getenv("METAAPI_ACCOUNT_ID")
port = 10000

# Global state tracker for connected mobile apps
connected_clients = set()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    print(f"Mobile app connected. Active clients: {len(connected_clients)}")
    try:
        while True:
            # Keep connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.remove(websocket)
        print(f"Mobile app disconnected. Active clients: {len(connected_clients)}")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(stream_market_data())

async def stream_market_data():
    print("Initializing MetaApi connection platform...")
    api = MetaApi(token)
    try:
        account = await api.metatrader_account_api.get_account(account_id)
        
        if account.state != 'DEPLOYED':
            print("Waiting for MetaApi account deployment...")
            await account.deploy()
        
        await account.wait_managed_connect()
        connection = account.get_rpc_connection()
        await connection.connect()
        await connection.wait_synchronization()
        print("Successfully synchronized with MetaTrader Core Platform.")

        # Main streaming data loop
        while True:
            if connected_clients:
                try:
                    # 1. Fetch Raw Ticks for accurate UI execution
                    tick = await connection.get_ticket(symbol="XAUUSD")
                    raw_price = tick['ask']
                    
                    # --- AUTO-CALIBRATION MATH BLOCK ---
                    # Pass raw market price directly to maintain perfect terminal synchronization
                    calibrated_price = round(raw_price, 2)

                    # Broadcast live data payload directly to connected mobile apps
                    payload = {
                        "price": calibrated_price,
                        "timestamp": tick.get('time', 0)
                    }
                    
                    # Send payload to all connected frontend channels
                    for client in list(connected_clients):
                        try:
                            await client.send_json(payload)
                        except Exception:
                            connected_clients.remove(client)

                except Exception as e:
                    print(f"Error reading market stream tick: {e}")
                    
            await asyncio.sleep(1)  # Throttle loop to protect connection channels

    except Exception as e:
        print(f"Critical initialization error: {e}")
