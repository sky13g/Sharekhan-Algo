import os
import json
import asyncio
import random
from datetime import datetime, time as datetime_time, timezone, timedelta
import pandas as pd
import requests
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
from dotenv import load_dotenv

load_dotenv()

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
NIFTY_INDEX_SCRIP_ID = 25000001  # Replace with exact Sharekhan Scrip Code for Nifty Spot
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
is_trading_paused = True        # Locked until dynamic token authorization completes
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
    try: requests.post(url, json={"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=5)
    except Exception: pass

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
        except Exception as e:
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
                return instruments[0].get("scripCode")
    except Exception:
        pass
    return None

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
                        await execute_order_async("SELL", current_position, spot_now, 0.0, 0.0, 0.0, "🔴 EMERGENCY PANIC SQUARE-OFF")
        except Exception: pass
        await asyncio.sleep(2)

# ==============================================================================
# 4. PAPER ORDER EXECUTOR MATRIX & MATH FILTERS
# ==============================================================================
async def execute_order_async(transaction_type, option_type, spot_price, er, r_high, r_low, condition):
    global total_net_pnl, peak_pnl, max_drawdown_cash, active_trade_entry_premium, last_action_status, active_contract_scrip_code, current_position
    timestamp_str = datetime.now().strftime('%H:%M:%S')
    if transaction_type == "BUY":
        target_strike = (round(spot_price / 50) * 50) - OPTION_OFFSET if option_type == "CE" else (round(spot_price / 50) * 50) + OPTION_OFFSET
        active_contract_scrip_code = get_active_options_scrip_code(target_strike, option_type)
    if not active_contract_scrip_code: return False
    live_premium_price = await fetch_live_option_premium_safe(active_contract_scrip_code) or 150.0
    if transaction_type == "BUY":
        active_trade_entry_premium = live_premium_price
        current_position = option_type
    send_telegram_alert(f"📝 *SHAREKHAN PAPER ORDER*\nAction: {transaction_type}\nType: {option_type}\nPremium: ₹{live_premium_price}\nReason: {condition}")
    last_action_status = f"📄 Logged {transaction_type} {option_type} at Premium ₹{live_premium_price}"
    if transaction_type == "SELL" and active_trade_entry_premium > 0:
        trade_pnl = (live_premium_price - active_trade_entry_premium) * QTY
        total_net_pnl += trade_pnl
        if total_net_pnl > peak_pnl: peak_pnl = total_net_pnl
        if (peak_pnl - total_net_pnl) > max_drawdown_cash: max_drawdown_cash = peak_pnl - total_net_pnl
  
