import os
import json
import asyncio
import random
from datetime import datetime, timezone
import pandas as pd
import requests
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from contextlib import asynccontextmanager
from dotenv import load_dotenv

load_dotenv()

# ==============================================================================
# LIFESPAN BACKGROUND CONTROLLER (Manages asynchronous loops on startup)
# ==============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global is_engine_running
    is_engine_running = True
    # Spawns your background Telegram loop concurrently with Uvicorn
    asyncio.create_task(telegram_command_listener_loop())
    yield
    is_engine_running = False

# Instantiate global FastAPI instance with lifespan context binding
app = FastAPI(
    title="Sharekhan Algorithmic Trading Engine",
    version="1.0.0",
    lifespan=lifespan
)

# ==============================================================================
# 1. SHAREKHAN & TELEGRAM CONFIGURATION
# ==============================================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

SK_API_KEY = os.getenv("SHAREKHAN_API_KEY")
SK_SECRET_KEY = os.getenv("SHAREKHAN_SECRET_KEY")
SK_CONSUMER_ID = os.getenv("SHAREKHAN_CONSUMER_ID")

# Strategy Primitives
TICKER_SYMBOL = "NIFTY"
NIFTY_INDEX_SCRIP_ID = 25000001  
LOT_SIZE = 65
QTY = 1 * LOT_SIZE
OPTION_OFFSET = 800

# Core State Matrix
current_position = "NONE"       
last_trade_time = None
ER_LOOKBACK = 10
CHANNEL_LOOKBACK = 15
live_vix_value = 15.0
trailing_sl_active = False
highest_observed_spot = 0.0
lowest_observed_spot = 999999.0
TRAILING_SL_POINTS = 15.0
candle_history_df = pd.DataFrame()

# Balance Sheet Tracking Variables
total_net_pnl = 0.0
peak_pnl = 0.0
max_drawdown_cash = 0.0
active_trade_entry_premium = 0.0
active_contract_scrip_code = None
last_action_status = "🤖 Sharekhan Engine Active. Awaiting login Token Activation..."
recent_trades_ledger = []

is_engine_running = False
is_trading_paused = True        
connected_clients = []
last_telegram_update_id = 0

BASE_URL = "https://sharekhan.com"
access_token = None

# Network Backoff Bounds
MAX_RETRIES = 5
BASE_DELAY_SECONDS = 2.0

# ==============================================================================
# 2. SHAREKHAN REST API NETWORK CONNECTIONS
# ==============================================================================
def send_telegram_alert(message):
    if not TELEGRAM_TOKEN or not CHAT_ID: return
    url = f"https://telegram.org{TELEGRAM_TOKEN}/sendMessage"
    try: 
        requests.post(url, json={"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=5)
    except Exception: 
        pass

def process_sharekhan_session_generation(request_token):
    """Exchanges dynamic user request login redirects for persistent secure access token"""
    global access_token, last_action_status, is_trading_paused
    url = f"{BASE_URL}/access/token"
    headers = {"apiKey": SK_API_KEY, "Content-Type": "application/json"}
    payload = {
        "secretKey": SK_SECRET_KEY,
        "requestToken": request_token,
        "consumerId": SK_CONSUMER_ID
    }
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        data = response.json()
        if response.status_code == 200 and "accessToken" in data:
            access_token = data["accessToken"]
            is_trading_paused = False
            last_action_status = "▶️ Running Live: Sharekhan session authenticated."
            send_telegram_alert("✅ *SHAREKHAN ALGO SESSION ACTIVE*\nTrading engine is fully unpaused and parsing data channels.")
        else:
            last_action_status = f"❌ Authentication Rejected: {data.get('message', 'Unknown Error')}"
            send_telegram_alert(f"❌ *SHAREKHAN AUTH REJECTED*\nDetails: `{data.get('message', 'Error Parsing payload context')}`")
    except Exception as e:
        last_action_status = f"❌ Connection Error: {e}"

async def execute_with_retry(api_call_func):
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            return api_call_func()
        except Exception:
            attempt += 1
            delay = (BASE_DELAY_SECONDS ** attempt) + random.uniform(0.5, 1.5)
            await asyncio.sleep(delay)
    return None

async def fetch_live_spot_price_safe():
    """Fetches Live Tick feeds using custom header map parameters"""
    if not access_token: return None
    def _call():
        url = f"{BASE_URL}/feeds/market/{NIFTY_INDEX_SCRIP_ID}"
        headers = {"apiKey": SK_API_KEY, "accessToken": access_token, "Content-Type": "application/json"}
        res = requests.get(url, headers=headers, timeout=5)
        if res.status_code == 200:
            return float(res.json().get("data", {}).get("ltp", 0.0))
        return None
    return await execute_with_retry(_call)

async def fetch_live_option_premium_safe(contract_code):
    if not access_token or not contract_code: return None
    def _call():
        url = f"{BASE_URL}/feeds/market/{contract_code}"
        headers = {"apiKey": SK_API_KEY, "accessToken": access_token, "Content-Type": "application/json"}
        res = requests.get(url, headers=headers, timeout=5)
        if res.status_code == 200:
            return float(res.json().get("data", {}).get("ltp", 0.0))
        return None
    return await execute_with_retry(_call)

def get_active_options_scrip_code(strike_price, option_type):
    """Maps options parameters into specific Sharekhan unique identification codes"""
    if not access_token: return None
    url = f"{BASE_URL}/instruments/search"
    headers = {"apiKey": SK_API_KEY, "accessToken": access_token, "Content-Type": "application/json"}
    payload = {"search_text": f"NIFTY {strike_price} {option_type}"}
    try:
        res = requests.post(url, headers=headers, json=payload, timeout=5)
        if res.status_code == 200:
            instruments = res.json().get("data", [])
            if len(instruments) > 0:
                return instruments.get("scripCode")
    except Exception:
        pass
    return None

async def execute_order_async(action, position, spot, target, sl, trailing, label):
    """Fallback placeholder logic handling internal trade router mappings"""
    global last_action_status
    last_action_status = f"⚡ Order Triggered: {action} {position} via {label}"
    send_telegram_alert(f"🔔 *TRADE SIGNAL*: {last_action_status} | Spot: `{spot}`")
    await asyncio.sleep(0.1)

# ==============================================================================
# 3. INTERACTIVE COMMUNICATIONS CONTROLLER
# ==============================================================================
async def telegram_command_listener_loop():
    global is_trading_paused, last_telegram_update_id, last_action_status, current_position
    if not TELEGRAM_TOKEN: return
    url = f"https://telegram.org{TELEGRAM_TOKEN}/getUpdates"
    while is_engine_running:
        try:
            params = {"offset": last_telegram_update_id + 1, "timeout": 8}
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(None, lambda: requests.get(url, params=params, timeout=10).json())
            if response.get("ok") and response.get("result"):
                for update in response["result"]:
                    last_telegram_update_id = update["update_id"]
                    message = update.get("message", {})
                    text = message.get("text", "").strip()
                    sender_chat_id = str(message.get("chat", {}).get("id", ""))
                    if sender_chat_id != str(CHAT_ID): continue
                    
                    if text.startswith("/token "):
                        raw_token = text.replace("/token ", "").strip()
                        send_telegram_alert("📥 Processing dynamic authorization token payload...")
                        process_sharekhan_session_generation(raw_token)
                    elif text == "/pause":
                        is_trading_paused = True
                        last_action_status = "⏸️ Engine paused via user intervention."
                        send_telegram_alert("⏸️ *TRADING HALTED*")
                    elif text == "/resume":
                        is_trading_paused = False
                        last_action_status = "▶️ Hunting setups live."
                        send_telegram_alert("▶️ *TRADING RESUMED*")
                    elif text == "/status":
                        msg = f"🤖 *SHAREKHAN METRICS*\n• Exposure: `{current_position}`\n• Balance: `₹{total_net_pnl:,.2f}`\n• Track: `{last_action_status}`"
                        send_telegram_alert(msg)
                    elif text == "/panic" and current_position != "NONE":
                        spot_now = await fetch_live_spot_price_safe() or 22000.0
                        await execute_order_async("SELL", current_position, spot_now, 0.0, 0.0, 0.0, "🔴 EMERGENCY PANIC CLOSE")
        except Exception:
            await asyncio.sleep(5)

# ==============================================================================
# 4. HTTP AUTOMATED AUTHENTICATION & WEBSOCKET ENDPOINTS
# ==============================================================================
@app.get("/")
async def root_gateway_endpoint():
    """Serves home route matrix metrics, satisfying Render's default health checking pings"""
    return JSONResponse(status_code=200, content={
        "status": "Running Live",
        "engine": "Sharekhan Algorithmic Strategic Controller",
        "system_time_utc": str(datetime.now(timezone.utc)),
        "engine_active": is_engine_running,
        "trading_paused": is_trading_paused,
        "current_exposure": current_position,
        "net_pnl_cash": total_net_pnl,
        "last_telemetry_status": last_action_status
    })  # <--- MAKE SURE THIS SAYS }) TO CLOSE THE DICTIONARY AND FUNCTION!
from fastapi import Request

@app.post("/auth/postback")
async def sharekhan_postback_receiver(request: Request):
    """Listens for execution, rejection, and modification status alerts pushed from Sharekhan"""
    try:
        # Reads the raw incoming payload structure pushed from the broker
        payload = await request.json()
        
        # Stream the update message directly into your Render server dashboard log terminal
        print(f"📥 [Postback Alert] Order Status Update Received: {json.dumps(payload)}")
        
        # Parse critical order keys from the payload mapping matrix
        order_id = payload.get("orderId", "N/A")
        order_status = payload.get("status", "UNKNOWN")
        scrip_code = payload.get("scripCode", "N/A")
        trade_action = payload.get("action", "N/A")  # BUY or SELL
        
        # Send a formatted, instant notification to your Telegram tracking bot channel
        alert_msg = (
            f"🔔 *SHAREKHAN ORDER UPDATE*\n"
            f"• ID: `{order_id}`\n"
            f"• Asset Code: `{scrip_code}`\n"
            f"• Action: *{trade_action}*\n"
            f"• Current Status: `{order_status}`"
        )
        send_telegram_alert(alert_msg)
        
        # Return a clean HTTP 200 response to acknowledge receipt to the broker
        return JSONResponse(status_code=200, content={"status": "Success", "message": "Postback logged cleanly"})
        
    except Exception as e:
        print(f"❌ Failed to parse incoming execution postback payload: {e}")
        return JSONResponse(status_code=400, content={"status": "Error", "message": str(e)})
        
