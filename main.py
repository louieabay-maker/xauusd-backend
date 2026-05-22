import os
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from metaapi_cloud_sdk import MetaApi

# Initialize FastAPI App
app = FastAPI()

# Retrieve Environment Variables from Render Config
token = os.getenv("METAAPI_TOKEN")
account_id = os.getenv("METAAPI_ACCOUNT_ID")

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
    
    # Clean up token and account ID strings safely
    api_token = str(token).strip() if token else ""
    target_account_id = str(account_id).strip() if account_id else ""
    
    if not api_token or not target_account_id:
        print("Critical Error: METAAPI_TOKEN or METAAPI_ACCOUNT_ID environment variables are missing!")
        return

    api = MetaApi(api_token)
    try:
        # Dynamically load the exact Account ID from your environment settings
        account = await api.metatrader_account_api.get_account(target_account_id)
        
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
                    # Fetch Raw Ticks with zero offset math adjustments
                    tick = await connection.get_ticket(symbol="XAUUSD")
                    raw_price = tick['ask']
                    calibrated_price = round(raw_price, 2)

                    # Build the live data payload package cleanly
                    payload = {
                        "price": calibrated_price,
                        "timestamp": tick.get('time', 0)
                    }
                    
                    # Broadcast to all connected frontend channels
                    for client in list(connected_clients):
                        try:
                            await client.send_json(payload)
                        except Exception:
                            connected_clients.remove(client)

                except Exception as e:
                    print(f"Error reading market stream tick: {e}")
                    
            await asyncio.sleep(1)  # 1-second interval loop throttle

    except Exception as e:
        print(f"Critical initialization error: {e}")
