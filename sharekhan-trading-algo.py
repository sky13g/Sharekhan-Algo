import os  
import sys  
import io
import asyncio  
from datetime import datetime, time as datetime_time, timezone, timedelta
import pandas as pd  
import requests  
import struct
import json
import websockets
from fastapi import FastAPI, Request  
from contextlib import asynccontextmanager  
from dotenv import load_dotenv 

load_dotenv() 

# ==============================================================================
# 1. CONFIGURATION & STATE FLAGS
# ==============================================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")  
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL")  

SHAREKHAN_API_KEY = os.getenv("SHAREKHAN_API_KEY")
SHAREKHAN_SECRET_KEY = os.getenv("SHAREKHAN_SECRET_KEY")
SHAREKHAN_LOGIN_ID = os.getenv("SHAREKHAN_LOGIN_ID")
SHAREKHAN_PASSWORD = os.getenv("SHAREKHAN_PASSWORD")

SHAREKHAN_BASE_URL = "https://sharekhan.com"
SHAREKHAN_STREAM_URL = "wss://://sharekhan.com"
SESSION_TOKEN_FILE = "/tmp/sharekhan_session.txt"

TICKER_SYMBOL = "^NSEI"  
LOT_SIZE = 75  
ACTIVE_LOTS = 1  
QTY = ACTIVE_LOTS * LOT_SIZE  

STOP_LOSS_PERC = 0.10      
TAKE_PROFIT_PERC = 0.20    
TRAILING_SL_PERC = 0.05    

current_position = "NONE"  
active_trade_details = {"symbol": None, "scrip_code": None, "entry_price": 0.0, "highest_price": 0.0, "stop_loss": 0.0, "take_profit": 0.0, "option_type": None}

trade_history_ledger = []
daily_analytics_summary = {"total_trades": 0, "winning_trades": 0, "losing_trades": 0, "gross_pnl": 0.0}

# Global Memory Variables for Real-Time Analytics
cached_spot = 0.0
cached_ema5 = 0.0
cached_ema10 = 0.0
rolling_tick_prices = []  # Stores high-frequency raw tick floats
minute_close_history = []  # Acts as an in-memory database of 1-minute close prices

access_token = None
is_authenticated = False
otp_received_event = asyncio.Event()
latest_submitted_otp = None
request_token_cache = None
scrip_master_df = None

# ==============================================================================
# 2. FASTAPI INITIALIZATION & GLOBAL ROUTES
# ==============================================================================
app = FastAPI() 

@app.get("/")
async def homepage_health_check():
    ist_zone = timezone(timedelta(hours=5, minutes=30))
    return {
        "status": "Online",
        "engine": "Sharekhan Ultra-Low Latency Tick Engine",
        "current_time_ist": datetime.now(ist_zone).strftime('%Y-%m-%d %H:%M:%S'),
        "broker_authenticated": is_authenticated,
        "active_exposure": current_position,
        "live_spot_ltp": cached_spot,
        "ema_5": round(cached_ema5, 2),
        "ema_10": round(cached_ema10, 2),
        "candles_in_memory": len(minute_close_history)
    }

@app.post("/telegram-webhook")
async def process_telegram_incoming_message(request: Request):
    global latest_submitted_otp
    try:
        payload = await request.json()
        if "message" not in payload or "text" not in payload["message"]:
            return {"status": "ignored"}
            
        incoming_chat_id = str(payload["message"]["chat"]["id"])
        msg_text = str(payload["message"]["text"]).strip()

        if incoming_chat_id != str(CHAT_ID):
            return {"status": "unauthorized"}

        if not is_authenticated and msg_text.isdigit() and (4 <= len(msg_text) <= 6):
            latest_submitted_otp = msg_text
            otp_received_event.set()  
            return {"status": "otp_captured"}

        if msg_text == "/status":
            execute_status_broadcast()
            return {"status": "command_handled"}
            
        if msg_text.startswith("/lot "):
            execute_lot_rescale(msg_text)
            return {"status": "command_handled"}
            
        if msg_text == "/squareoff":
            execute_manual_squareoff()
            return {"status": "command_handled"}

    except Exception as e: 
        print(f"Webhook Execution Exception: {e}")
    return {"status": "processed"}

# ==============================================================================
# 3. TELEGRAM UTILITY & WEBHOOK INITIALIZATION
# ==============================================================================
def send_telegram_alert(message):  
    if not TELEGRAM_TOKEN or not CHAT_ID:  
        print(f"[Telegram Log]: {message}")  
        return  
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"  
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}  
    try: 
        requests.post(url, json=payload, timeout=5)
    except Exception as e: 
        print(f"[WARNING] Telegram post failed: {e}") 

def setup_telegram_webhook():
    if not TELEGRAM_TOKEN or not RENDER_URL: return
    webhook_endpoint = f"{RENDER_URL.rstrip('/')}/telegram-webhook"
    url = f"https://api.telegram.org{TELEGRAM_TOKEN}/setWebhook"
    try: 
        requests.post(url, json={"url": webhook_endpoint}, timeout=5)
    except Exception: 
        pass

# ==============================================================================
# 4. TEXT-BASED VISUAL CHART GENERATOR
# ==============================================================================
def generate_text_chart():
    global cached_spot, cached_ema5, cached_ema10
    if cached_spot == 0:
        return "No chart metrics compiled yet."
        
    mapping = [
        {"label": "NIFTY SPOT", "val": cached_spot, "char": "🔹"},
        {"label": "EMA 5 (Fast)", "val": cached_ema5, "char": "🟢"},
        {"label": "EMA 10 (Slow)", "val": cached_ema10, "char": "🟠"}
    ]
    mapping.sort(key=lambda x: x["val"], reverse=True)
    
    chart_lines = ["\n📈 *TREND VISUALIZER* (Real-Time Websocket Close)"]
    for item in mapping:
        chart_lines.append(f"│  {item['char']}  {item['label']}: {round(item['val'], 2)}")
    
    direction = "🟢 BULLISH CROSS (UPTREND)" if cached_ema5 > cached_ema10 else "🔴 BEARISH CROSS (DOWNTREND)"
    chart_lines.append(f"└▶ State: {direction}")
    
    return "\n".join(chart_lines)

# ==============================================================================
# 5. AUTOMATED MASTER SYNC & CONTRACT EXTRACTOR
# ==============================================================================
def download_scrip_master():
    global scrip_master_df
    print("[SYSTEM] Fetching dynamic Sharekhan Scrip Master file...")
    url = "https://sharekhan.com"
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            raw_data = io.StringIO(response.text)
            df = pd.read_csv(raw_data, sep="|", low_memory=False)
            scrip_master_df = df[df['Exchange'] == 'NCED']
            print(f"[SYSTEM] Master parsed successfully. Loaded {len(scrip_master_df)} contracts.")
    except Exception as e:
        print(f"[CRITICAL ERROR] Failed to fetch Sharekhan Master data: {e}")

def get_option_contract_details(spot_price, option_type):
    global scrip_master_df
    strike_price = int(round(spot_price / 50) * 50)
    now = datetime.now()
    expiry_month_str = now.strftime("%b").upper() 
    expiry_year_short = now.strftime("%y")       
    trading_symbol = f"NIFTY{expiry_year_short}{expiry_month_str}{strike_price}{option_type}"
    scrip_code = 43501 
    
    if scrip_master_df is not None and not scrip_master_df.empty:
        try:
            matched = scrip_master_df[
                (scrip_master_df['BaseSymbol'] == 'NIFTY') & 
                (scrip_master_df['StrikePrice'] == strike_price) & 
                (scrip_master_df['OptionType'] == option_type)
            ]
            if not matched.empty:
                scrip_code = int(matched.iloc['ScripCode'].values)
                trading_symbol = str(matched.iloc['TradingSymbol'].values)
        except Exception as err:
            print(f"[LOOKUP WARNING] Pattern lookup failed: {err}")
    return {"symbol": trading_symbol, "scrip_code": scrip_code}

# ==============================================================================
# 6. ROUTED EXECUTION HANDLERS
# ==============================================================================
def execute_status_broadcast():
    pnl_val = round(daily_analytics_summary["gross_pnl"], 2)
    chart_v = generate_text_chart()
    if current_position == "NONE":
        send_telegram_alert(f"ℹ️ *ALGO ENGINE STATUS*\n• State: Flat\n• Lots: {ACTIVE_LOTS}\n• PnL: ₹{pnl_val}\n{chart_v}")
        return
    live_ltp = get_sharekhan_live_ltp(active_trade_details["scrip_code"]) or active_trade_details["entry_price"]
    send_telegram_alert(f"ℹ️ *ALGO ENGINE STATUS*\n• Holding: {current_position}\n• Symbol: {active_trade_details['symbol']}\n• Entry: ₹{active_trade_details['entry_price']}\n• LTP: ₹{live_ltp}\n• SL: ₹{active_trade_details['stop_loss']}\n• Target: ₹{active_trade_details['take_profit']}\n• PnL: ₹{pnl_val}\n{chart_v}")

def execute_lot_rescale(msg_text):
    global ACTIVE_LOTS, QTY
    raw_num = msg_text.replace("/lot ", "").strip()
    if not str(raw_num).isdigit():
        send_telegram_alert("⚠️ Use format configuration: `/lot 3`")
        return
    requested_lots = int(raw_num)
    if requested_lots >= 1 and requested_lots <= 20:
        ACTIVE_LOTS = requested_lots
        QTY = ACTIVE_LOTS * LOT_SIZE
        send_telegram_alert(f"⚙️ *Lots Updated!* Sizing: **{ACTIVE_LOTS} Lots** ({QTY} Qty).")
        return
    send_telegram_alert("⚠️ Select a lot size parameter between 1 and 20.")

def execute_manual_squareoff():
    global current_position
    if current_position == "NONE":
        send_telegram_alert("⚠️ Portfolio risk management layers are flat.")
        return
    sq_ltp = get_sharekhan_live_ltp(active_trade_details["scrip_code"]) or active_trade_details["entry_price"]
    execute_paper_order("SELL (MANUAL OVERRIDE)", active_trade_details, sq_ltp)
