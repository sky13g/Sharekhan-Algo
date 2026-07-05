import os
import time
from datetime import datetime
import pytz
import requests
from dotenv import load_dotenv

# Assuming standard SmartConnect protocol used by Sharekhan automation wrappers
# Install via: pip install smartapi-python pytz requests
from SmartApi import SmartConnect 

load_dotenv()

class SharekhanAlgoController:
    def __init__(self):
        # 1. FIX: Force Render environment variables or fallback to secure config
        self.api_key = os.getenv("SHAREKHAN_API_KEY", "your_api_key_here")
        self.client_code = os.getenv("SHAREKHAN_CLIENT_CODE", "your_client_code")
        self.password = os.getenv("SHAREKHAN_PASSWORD", "your_password")
        self.totp_key = os.getenv("SHAREKHAN_TOTP_KEY", "your_totp_secret")
        
        self.obj = None
        self.session_data = None
        self.system_status = {
            "status": "Initializing",
            "engine": "Sharekhan Algorithmic Strategic Controller",
            "system_time_utc": "",
            "engine_active": True,
            "trading_paused": True,
            "current_exposure": "NONE",
            "net_pnl_cash": 0,
            "last_telemetry_status": "🤖 Initializing system..."
        }

    def update_telemetry(self, status_msg, paused=True):
        """Helper to keep logs perfectly aligned with your dashboard format"""
        tz_utc = pytz.utc
        current_time = datetime.now(tz_utc).strftime('%Y-%m-%d %H:%M:%S.%f%z')
        
        self.system_status["system_time_utc"] = current_time
        self.system_status["trading_paused"] = paused
        self.system_status["last_telemetry_status"] = status_msg
        print(f"TELEMETRY UPDATE: {self.system_status}")

    def fix_render_wake_up(self):
        """2. FIX: Prevent Render free-tier web services from spinning down"""
        self.update_telemetry("🤖 Keeping Render Instance Awake...", paused=True)
        try:
            # Self-pinging the URL provided ensures the container stays warm
            render_url = "https://sharekhan-algo.onrender.com/"
            response = requests.get(render_url, timeout=10)
            if response.status_code == 200:
                print("Self-ping successful. Container is awake.")
        except Exception as e:
            print(f"Self-ping failed (Can ignore if running locally): {e}")

    def generate_and_authenticate_token(self):
        """3. FIX: Authenticate API, generate Token, and unpause trading"""
        self.update_telemetry("🤖 Sharekhan Engine Active. Awaiting login Token Activation...", paused=True)
        
        try:
            # Initialize the broker session connector
            self.obj = SmartConnect(api_key=self.api_key)
            
            # Generate TOTP using your secret key setup
            import pyotp
            totp = pyotp.TOTP(self.totp_key).now()
            
            # Authenticate session
            self.session_data = self.obj.generateSession(self.client_code, self.password, totp)
            
            if self.session_data.get('status') == True:
                # Extract the authentication token generated
                feed_token = self.obj.getfeedToken()
                
                # Update telemetry state to LIVE and UNPAUSED
                self.update_telemetry("🚀 Token Activated. Connection Live. Trading Resumed!", paused=False)
                return True
            else:
                self.update_telemetry("❌ Login Failed. Check credentials or TOTP secret key.", paused=True)
                return False
                
        except Exception as e:
            self.update_telemetry(f"❌ Critical Auth Error: {str(e)}", paused=True)
            return False

    def is_market_open_ist(self):
        """4. FIX: Accurate Indian Standard Time (IST) execution filter"""
        # Convert system time reliably to Asia/Kolkata
        tz_utc = pytz.utc
        tz_ist = pytz.timezone('Asia/Kolkata')
        
        utc_now = datetime.now(tz_utc)
        ist_now = utc_now.astimezone(tz_ist)
        
        # Check weekdays (0 = Monday, 6 = Sunday)
        if ist_now.weekday() >= 5:
            return False
            
        # Market Hours: 09:15 to 15:30 IST
        market_start = ist_now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_end = ist_now.replace(hour=15, minute=30, second=0, microsecond=0)
        
        return market_start <= ist_now <= market_end

    def run_engine_loop(self):
        """Core execution loop"""
        # Keep instance awake first
        self.fix_render_wake_up()
        
        # Authenticate and resolve 'Awaiting login Token Activation...'
        if self.generate_and_authenticate_token():
            while not self.system_status["trading_paused"]:
                if self.is_market_open_ist():
                    print("Scanning markets and executing strategy...")
                    # Insert your core mathematical strategy/order placement execution logic here
                else:
                    print("System is Live, but Indian Markets are currently CLOSED. Waiting...")
                
                time.sleep(60) # Scan loop interval

# Execution
if __name__ == "__main__":
    controller = SharekhanAlgoController()
    controller.run_engine_loop()
    
