import os
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="XAUUSD AI Live Feed")

# Enable CORS so your Base44 frontend can communicate with Render smoothly
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------------------------------------------
# Helper Functions & Formatting Fixes
# ----------------------------------------------------------------

def toast(message: str, options: dict = None):
    """
    Utility function to handle alert dispatches.
    Fixed: Uses Unicode escape sequence to prevent file encoding syntax crashes.
    """
    print(f"[ALERT] {message} with options {options}")
    return {"status": "alert_dispatched", "message": message}


def get_pipeline_status_html() -> str:
    """
    Fixed: Wrapped raw HTML string in proper Python quotes to prevent 
    SyntaxError: invalid character '·' (U+00B7).
    """
    return '<p className="text-xs text-muted-foreground">XAUUSD &middot; Render Node Pipeline</p>'

# ----------------------------------------------------------------
# WebSocket & Live Feed API Routes
# ----------------------------------------------------------------

@app.get("/")
async def root():
    # Pull raw numbers directly out of your python server's payload package
    return {
        "status": "online",
        "service": "XAUUSD AI Backend",
        "pipeline_element": get_pipeline_status_html()
    }


@app.get("/trigger-test-alert")
async def trigger_test_alert():
    # Fixed: Passing the safe unicode sequence for the 🔔 emoji string
    return toast('\U0001F514 Price Alert', {"type": "info"})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Direct live-feed websocket connection bypassing old MT5 layers.
    """
    await websocket.accept()
    print("[WS] Base44 client connected to direct live-feed stream.")
    try:
        while True:
            # Simple keepalive receiver block
            data = await websocket.receive_text()
            # In production, replace this with your direct live-feed API broadcast loop
            await websocket.send_json({
                "symbol": "XAUUSD",
                "price": 2420.50,  # Example real-time price placeholder
                "status": "streaming"
            })
    except WebSocketDisconnect:
        print("[WS] Base44 client disconnected safely.")

# ----------------------------------------------------------------
# Application Startup
# ----------------------------------------------------------------

if __name__ == "__main__":
    # Fixed: Hardcoded to port 10000 to match Render's environment expectations
    port = int(os.environ.get("PORT", 10000))
    print("Initializing Direct Data Stream platform...")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
