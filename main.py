import asyncio, json, time
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from collections import deque
import numpy as np

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

SYMBOL = "XAU/USD"

# Shared state
price_history = deque(maxlen=100)
latest_payload = {}

def calc_ema(prices, period):
    if len(prices) < period:
        return prices[-1] if prices else 0
    k = 2 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = p * k + ema * (1 - k)
    return round(ema, 2)

def calc_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)

async def twelve_data_stream(api_key: str):
    """Connect to Twelve Data WebSocket and stream XAU/USD prices."""
    global latest_payload
    uri = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={api_key}"

    while True:
        try:
            async with websockets.connect(uri) as ws:
                await ws.send(json.dumps({
                    "action": "subscribe",
                    "params": {"symbols": SYMBOL}
                }))
                print(f"✅ Connected to Twelve Data — streaming {SYMBOL}")

                async for message in ws:
                    data = json.loads(message)
                    if data.get("event") != "price":
                        continue

                    price = float(data["price"])
                    ts = data.get("timestamp", int(time.time()))
                    time_str = time.strftime("%H:%M:%S", time.localtime(ts))

                    price_history.append(price)
                    prices = list(price_history)

                    ema9  = calc_ema(prices, 9)
                    ema21 = calc_ema(prices, 21)
                    rsi   = calc_rsi(prices)

                    if rsi < 30 and ema9 > ema21:
                        prediction, reason = "BUY Prediction", "RSI oversold + EMA bullish cross"
                    elif rsi > 70 and ema9 < ema21:
                        prediction, reason = "SELL Prediction", "RSI overbought + EMA bearish cross"
                    else:
                        prediction, reason = "HOLD", "No clear signal"

                    chart_data = [{"price": p, "time": ""} for p in prices[-60:]]
                    chart_data[-1]["time"] = time_str

                    latest_payload = {
                        "price":      price,
                        "rsi":        rsi,
                        "ema9":       ema9,
                        "ema21":      ema21,
                        "prediction": prediction,
                        "reason":     f"[Live] {reason}",
                        "tp":         round(price + (price * 0.006), 2),
                        "sl":         round(price - (price * 0.003), 2),
                        "chartData":  chart_data,
                    }

        except Exception as e:
            print(f"❌ Stream error: {e} — reconnecting in 5s")
            await asyncio.sleep(5)

# Connected clients
clients: list[WebSocket] = []
stream_started = False

@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    td_key: str = Query(default="")   # ← receives key from your app
):
    global stream_started
    await websocket.accept()
    clients.append(websocket)

    # Start the Twelve Data stream on first connection
    if not stream_started and td_key:
        stream_started = True
        asyncio.create_task(twelve_data_stream(td_key))
        asyncio.create_task(broadcast_loop())
        print(f"🚀 Stream started with provided API key")

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        clients.remove(websocket)

async def broadcast_loop():
    while True:
        if latest_payload and clients:
            dead = []
            for ws in clients:
                try:
                    await ws.send_text(json.dumps(latest_payload))
                except:
                    dead.append(ws)
            for ws in dead:
                clients.remove(ws)
        await asyncio.sleep(1)
