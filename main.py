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
port = int(os.getenv("PORT", 10000))

# Global state tracker for connected mobile apps
connected_clients = set()

def calculate_ema(prices, period):
    """Helper function to calculate Exponential Moving Average (EMA)"""
    if len(prices) < period:
        return None
    return pd.Series(prices).ewm(span=period, adjust=False).mean().iloc[-1]

async def stream_market_data():
    if not token or not account_id:
        print("Missing MetaApi credentials in environment variables.")
        return

    api = MetaApi(token)
    try:
        account = await api.metatrader_account_api.get_meta_trader_account(account_id)
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
                    # Subtracts exactly 112.2 to align the data feed with terminal pricing
                    calibrated_price = round(raw_price - 112.2, 2)

                    # 2. Fetch 4-Hour (4H) historical candles for Top-Down Trend Confluence
                    candles = await connection.get_candles(
                        symbol="XAUUSD", 
                        timeframe="4h", 
                        limit=30
                    )
                    
                    h4_closes = [c['close'] for c in candles]
                    ema9_4h = calculate_ema(h4_closes, 9)
                    ema21_4h = calculate_ema(h4_closes, 21)

                    # Determine Macro Trend Structure
                    if ema9_4h and ema21_4h:
                        macro_trend = "BULLISH" if ema9_4h > ema21_4h else "BEARISH"
                    else:
                        macro_trend = "NEUTRAL"

                    # 3. Fetch Lower-Interval Data for Active Strategy Signals (e.g., 30M)
                    m30_candles = await connection.get_candles(symbol="XAUUSD", timeframe="30m", limit=14)
                    m30_closes = [c['close'] for c in m30_candles]
                    
                    # Formulate base mock signal condition (Replace with your custom logic variables if needed)
                    base_signal = "BUY" if m30_closes[-1] > m30_closes[-2] else "SELL"
                    final_signal = base_signal
                    reason = "Trend aligned with technical metrics."

                    # --- TOP-DOWN TREND CONFLUENCE FILTER ---
                    # Blocks counter-trend entries based on anchor 4H structure
                    if final_signal == "BUY" and macro_trend == "BEARISH":
                        final_signal = "HOLD"
                        reason = "Blocked by 4H Macro Trend"
                    elif final_signal == "SELL" and macro_trend == "BULLISH":
                        final_signal = "HOLD"
                        reason = "Blocked by 4H Macro Trend"

                    # Package telemetry data packet
                    payload = {
                        "price": calibrated_price,
                        "macro_trend": macro_trend,
                        "signal": final_signal,
                        "reason": reason
                    }

                    # Broadcast packet to all open Base44 instances
                    for client in list(connected_clients):
                        try:
                            await client.send_json(payload)
                        except Exception:
                            connected_clients.remove(client)

                except Exception as e:
                    print(f"Error handling live tick cycle: {str(e)}")

            await asyncio.sleep(1) # Frequency processing delimiter

    except Exception as error:
        print(f"MetaApi Bridge connection crashed: {str(error)}")

@app.on_event("startup")
async def startup_event():
    # Run the streaming background daemon worker loop
    asyncio.create_task(stream_market_data())

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket route that Base44 calls to tap into your live stream"""
    await websocket.accept()
    connected_clients.add(websocket)
    print(f"Base44 client app linked successfully. Total active streams: {len(connected_clients)}")
    try:
        while True:
            # Maintain active connection keepalive loop
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.remove(websocket)
        print("Base44 client app disconnected cleanly.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=port)
