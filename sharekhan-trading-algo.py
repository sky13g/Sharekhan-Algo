import os  
import sys  
import io
import asyncio  
from datetime import datetime, time as datetime_time, timezone, timedelta
import pandas as pd  
import requests  
import yfinance as yf  
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

cached_spot = 0.0
cached_ema5 = 0.0
cached_ema10 = 0.0

access_token = None
is_authenticated = False
otp_received_event = asyncio.Event()
latest_submitted_otp = None
request_token_cache = None
scrip_master_df = None

# ==============================================================================
# 2. TELEGRAM UTILITY & WEBHOOK INITIALIZATION
# ==============================================================================
def send_telegram_alert(message):  
    if not TELEGRAM_TOKEN or not CHAT_ID:  
        print(f"[Telegram Log]: {message}")  
        return  
    url = f"https://telegram.org{TELEGRAM_TOKEN}/sendMessage"  
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}  
    try: 
        requests.post(url, json=payload, timeout=5)
    except Exception as e: 
        print(f"[WARNING] Telegram post failed: {e}") 

def setup_telegram_webhook():
    if not TELEGRAM_TOKEN or not RENDER_URL: return
    webhook_endpoint = f"{RENDER_URL.rstrip('/')}/telegram-webhook"
    url = f"https://telegram.org{TELEGRAM_TOKEN}/setWebhook"
    try: 
        requests.post(url, json={"url": webhook_endpoint}, timeout=5)
    except Exception: 
        pass

# ==============================================================================
# 3. TEXT-BASED VISUAL CHART GENERATOR
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
    
    chart_lines = ["\n📈 *TREND VISUALIZER* (1m Close)"]
    for item in mapping:
        chart_lines.append(f"│  {item['char']}  {item['label']}: {round(item['val'], 2)}")
    
    direction = "🟢 BULLISH CROSS (UPTREND)" if cached_ema5 > cached_ema10 else "🔴 BEARISH CROSS (DOWNTREND)"
    chart_lines.append(f"└▶ State: {direction}")
    
    return "\n".join(chart_lines)

# ==============================================================================
# 4. AUTOMATED MASTER SYNC & CONTRACT EXTRACTOR
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
                scrip_code = int(matched.iloc[0]['ScripCode'])
                trading_symbol = str(matched.iloc[0]['TradingSymbol'])
        except Exception as err:
            print(f"[LOOKUP WARNING] Pattern lookup failed: {err}")
    return {"symbol": trading_symbol, "scrip_code": scrip_code}

# ==============================================================================
# 5. SHAREKHAN INTERACTIVE AUTHENTICATION
# ==============================================================================
async def authenticate_sharekhan_with_otp():
    global access_token, is_authenticated, latest_submitted_otp, request_token_cache
    try:
        if os.path.exists(SESSION_TOKEN_FILE):
            with open(SESSION_TOKEN_FILE, "r") as f:
                saved_token = f.read().strip()
            if saved_token:
                headers = {"api-key": SHAREKHAN_API_KEY, "access-token": saved_token}
                res = requests.get(f"{SHAREKHAN_BASE_URL}/profile", headers=headers, timeout=5)
                if res.status_code == 200:
                    access_token = saved_token
                    send_telegram_alert("🔄 *Sharekhan Feed Token Restored!* Running simulation parameters.")
                    is_authenticated = True
                    return True

        init_url = f"{SHAREKHAN_BASE_URL}/login"
        payload = {"apiKey": SHAREKHAN_API_KEY, "loginId": SHAREKHAN_LOGIN_ID, "password": SHAREKHAN_PASSWORD}
        init_res = requests.post(init_url, json=payload, timeout=5).json()
        
        if "data" in init_res and "requestToken" in init_res["data"]:
            request_token_cache = init_res["data"]["requestToken"]

        send_telegram_alert("🔑 *Sharekhan Auth Triggered!*\nReply with your 2FA OTP code to authorize live market feeds.")
        await otp_received_event.wait()

        validate_url = f"{SHAREKHAN_BASE_URL}/validateOTP"
        otp_payload = {"apiKey": SHAREKHAN_API_KEY, "requestToken": request_token_cache, "otp": latest_submitted_otp}
        token_res = requests.post(validate_url, json=otp_payload, timeout=5).json()

        if "data" in token_res and "accessToken" in token_res["data"]:
            access_token = token_res["data"]["accessToken"]
            with open(SESSION_TOKEN_FILE, "w") as f:
                f.write(access_token)
            send_telegram_alert("🚀 *Feed Auth Connected!* Starting Paper Trading strategy loop.")
            is_authenticated = True
            return True
    except Exception as e:
        send_telegram_alert(f"❌ *Auth Link Failure:* {e}")
        return False

# ==============================================================================
# 6. FASTAPI EXPOSED WEBHOOK & CHAT COMMANDS LISTENER
# ==============================================================================
app = FastAPI() 

@app.post("/telegram-webhook")
async def process_telegram_incoming_message(request: Request):
    global latest_submitted_otp, current_position, ACTIVE_LOTS, QTY
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
            pnl_val = round(daily_analytics_summary["gross_pnl"], 2)
            chart_v = generate_text_chart()
            
            if current_position == "NONE":
                msg = f"ℹ️ *ALGO ENGINE STATUS*\n• **Current State**: Flat (No Exposure)\n• **Active Size**: {ACTIVE_LOTS} Lot ({QTY} Qty)\n• **Realised Today**: ₹{pnl_val}\n{chart_v}"
                send_telegram_alert(msg)
            else:
                live_ltp = get_sharekhan_live_ltp(active_trade_details["scrip_code"]) or active_trade_details["entry_price"]
                msg = f"ℹ️ *ALGO ENGINE STATUS*\n• **Current State**: Holding `{current_position}`\n• **Active Size**: {ACTIVE_LOTS} Lot ({QTY} Qty)\n• **Instrument**: {active_trade_details['symbol']}\n• **Entry Price**: ₹{active_trade_details['entry_price']}\n• **Live Price**: ₹{live_ltp}\n• **Stop Loss**: ₹{active_trade_details['stop_loss']}\n• **Take Profit**: ₹{active_trade_details['take_profit']}\n• **Realised Today**: ₹{pnl_val}\n{chart_v}"
    
