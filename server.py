import os
import hmac
import hashlib
import calendar
from flask import abort
import time
from datetime import datetime, date, timedelta, timezone
import pytz
import csv
import io
import gzip
import urllib.request
import urllib.parse
import random
import threading
import concurrent.futures
from collections import defaultdict
import numpy as np
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from flask import Flask, jsonify, request, send_from_directory, render_template_string, redirect, url_for, session
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
CORS(app)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "CHANGE_THIS_SECRET_BEFORE_DEPLOYING")
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=31)

# Absolute Path Fix for SQLite Database to prevent 'no such table' errors
basedir = os.path.abspath(os.path.dirname(__file__))
db_path = os.getenv('DATABASE_PATH', os.path.join(basedir, 'users.db'))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + db_path
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# User Database Model with Robust Constraints
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password = db.Column(db.String(255), nullable=False)
    watchlist = db.Column(db.Text, default="")
    reset_otp = db.Column(db.String(6), nullable=True)
    otp_expiry = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Payment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    order_id = db.Column(db.String(100), unique=True, nullable=False)
    payment_id = db.Column(db.String(100), unique=True, nullable=True)
    plan = db.Column(db.String(20), nullable=False)
    amount_paise = db.Column(db.Integer, nullable=False)
    status = db.Column(db.String(20), default='created', nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Subscription(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), unique=True, nullable=False)
    plan = db.Column(db.String(20), nullable=False)
    expires_at = db.Column(db.DateTime, nullable=False)

PLAN_DETAILS = {'monthly': (49900, 1), 'quarterly': (119900, 3),
                'halfyearly': (249900, 6), 'yearly': (499900, 12)}
RAZORPAY_KEY_ID = os.getenv('RAZORPAY_KEY_ID', 'rzp_test_TzMD86pJB0Uyup').strip()
RAZORPAY_KEY_SECRET = os.getenv('RAZORPAY_KEY_SECRET', 'JDlXl5ux5thUXIxtooMkgNl1').strip()

def current_user():
    return User.query.filter_by(email=session.get('user', '')).first() if session.get('user') else None

def active_subscription(user):
    sub = Subscription.query.filter_by(user_id=user.id).first() if user else None
    return bool(sub and sub.expires_at > datetime.utcnow())

def add_months(start, months):
    year = start.year + (start.month - 1 + months) // 12
    month = (start.month - 1 + months) % 12 + 1
    day = min(start.day, calendar.monthrange(year, month)[1])
    return start.replace(year=year, month=month, day=day)

# Explicit Application Context with Guaranteed Table Creation
with app.app_context():
    try:
        db.create_all()
        print("✅ Database tables created and verified successfully at:", db_path)
    except Exception as e:
        print(f"⚠️ Database initialization error: {e}")

# Persistent HTTP Session Pool to prevent Windows Socket crashes
HTTP_SESSION = requests.Session()
adapter = HTTPAdapter(
    pool_connections=50,
    pool_maxsize=50,
    max_retries=Retry(total=3, backoff_factor=0.2, status_forcelist=[502, 503, 504])
)
HTTP_SESSION.mount("https://", adapter)
HTTP_SESSION.mount("http://", adapter)

UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiJDTDgwMDQiLCJqdGkiOiI2YTZlZGU0YTdkMDdkYzI0NTcxY2IwNjgiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlzRXh0ZW5kZWQiOnRydWUsImlhdCI6MTc4NTY1MDc2MiwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxODE3MjQ0MDAwfQ.QevX5BwRdiDzZNmuSc0CGqDZcN5VP1qK6GXbvziEAik").strip()

QUOTE_CACHE = {
    "FO": {"data": [], "last_updated": 0},
    "NIFTY500": {"data": [], "last_updated": 0},
    "MIDCAP": {"data": [], "last_updated": 0},
    "SMALLCAP": {"data": [], "last_updated": 0},
    "TOTALMARKET": {"data": [], "last_updated": 0}
}
ZONE_CACHE = {
    "FO": {"last_updated": {}},
    "NIFTY500": {"last_updated": {}},
    "MIDCAP": {"last_updated": {}},
    "SMALLCAP": {"last_updated": {}},
    "TOTALMARKET": {"last_updated": {}}
}
CANDLE_CACHE = {}
CANDLE_CACHE_LOCK = threading.RLock()
ZONE_CACHE_LOCK = threading.RLock()

MIDCAP_UNIVERSE_CACHE = []
SMALLCAP_UNIVERSE_CACHE = []
TOTAL_MARKET_UNIVERSE_CACHE = []
SMART_ORB_SIGNALS = []

MARKET_INDICES = [
    {"symbol": "NIFTY 50", "isin": "NSE_INDEX|Nifty 50"},
    {"symbol": "NIFTY BANK", "isin": "NSE_INDEX|Nifty Bank"},
    {"symbol": "SENSEX", "isin": "BSE_INDEX|SENSEX"},
    {"symbol": "NIFTY FIN SERVICE", "isin": "NSE_INDEX|Nifty Financial Services"},
    {"symbol": "NIFTY IT", "isin": "NSE_INDEX|Nifty IT"}
]

TARGET_FO_SYMBOLS = {
    "AUBANK": "Nifty Bank", "AXISBANK": "Nifty Bank", "BANDHANBNK": "Nifty Bank", "BANKBARODA": "Nifty Bank",
    "BANKINDIA": "Nifty Bank", "MAHABANK": "Nifty Bank", "CANBK": "Nifty Bank", "FEDERALBNK": "Nifty Bank",
    "HDFCBANK": "Nifty Bank", "ICICIBANK": "Nifty Bank", "IDFCFIRSTB": "Nifty Bank", "INDIANB": "Nifty Bank",
    "INDUSINDBK": "Nifty Bank", "KOTAKBANK": "Nifty Bank", "PNB": "Nifty Bank", "RBLBANK": "Nifty Bank",
    "SBIN": "Nifty Bank", "UNIONBANK": "Nifty Bank", "YESBANK": "Nifty Bank",
    "COFORGE": "Nifty IT", "HCLTECH": "Nifty IT", "INFY": "Nifty IT", "KPITTECH": "Nifty IT",
    "LTM": "Nifty IT", "CAMS": "Nifty IT", "MPHASIS": "Nifty IT", "OFSS": "Nifty IT","PERSISTENT": "Nifty IT", "TCS": "Nifty IT",
    "TECHM": "Nifty IT", "WIPRO": "Nifty IT",
    "ASHOKLEY": "Nifty Auto", "BAJAJ AUTO": "Nifty Auto", "BALKRISIND": "Nifty Auto", "BOSCHLTD": "Nifty Auto",
    "EICHERMOT": "Nifty Auto", "FORCEMOT": "Nifty Auto", "HEROMOTOCO": "Nifty Auto", "HYUNDAI": "Nifty Auto",
    "M&M": "Nifty Auto", "MARUTI": "Nifty Auto", "MOTHERSON": "Nifty Auto", "SONACOMS": "Nifty Auto",
    "TATAMOTORS": "Nifty Auto", "TMPVL": "Nifty Auto", "TIINDIA": "Nifty Auto", "TVSMOTOR": "Nifty Auto",
    "UNOMINDA": "Nifty Auto",
    "ALKEM": "Nifty Pharma", "PPLPHARMA": "Nifty Pharma", "APOLLOHOSP": "Nifty Pharma", "AUROPHARMA": "Nifty Pharma", "BIOCON": "Nifty Pharma",
    "CIPLA": "Nifty Pharma", "DIVISLAB": "Nifty Pharma", "DRREDDY": "Nifty Pharma", "FORTIS": "Nifty Pharma",
    "GLENMARK": "Nifty Pharma", "LAURUSLABS": "Nifty Pharma", "LUPIN": "Nifty Pharma", "MANKIND": "Nifty Pharma",
    "MAXHEALTH": "Nifty Pharma", "SUNPHARMA": "Nifty Pharma", "TORNTPHARM": "Nifty Pharma", "ZYDUSLIFE": "Nifty Pharma",
    "HINDALCO": "Nifty Metal", "HINDZINC": "Nifty Metal", "JINDALSTEL": "Nifty Metal",
    "JSWSTEEL": "Nifty Metal", "NATIONALUM": "Nifty Metal", "NMDC": "Nifty Metal", "SAIL": "Nifty Metal",
    "TATASTEEL": "Nifty Metal", "VEDL": "Nifty Metal", "APLAPOLLO": "Nifty Metal",
    "COALINDIA": "Nifty Energy", "ADANIENSOL": "Nifty Energy", "ADANIGREEN": "Nifty Energy", "ADANIPOWER": "Nifty Energy", "BPCL": "Nifty Energy",
    "BHEL": "Nifty Energy", "CGPOWER": "Nifty Energy", "GAIL": "Nifty Energy", "GET&D": "Nifty Energy",
    "HINDPETRO": "Nifty Energy", "IEX": "Nifty Energy", "IOC": "Nifty Energy", "IREDA": "Nifty Energy",
    "JSWENERGY": "Nifty Energy", "NHPC": "Nifty Energy", "NTPC": "Nifty Energy", "ONGC": "Nifty Energy",
    "OIL": "Nifty Energy", "POWERGRID": "Nifty Energy", "PREMIERENE": "Nifty Energy", "RELIANCE": "Nifty Energy",
    "SUZLON": "Nifty Energy", "TATAPOWER": "Nifty Energy", "WAAREEENER": "Nifty Energy", "INOXWIND": "Nifty Energy",
    "PETRONET": "Nifty Energy", "POWERINDIA": "Nifty Energy",
    "360ONE": "Fin Service", "ABCAPITAL": "Fin Service", "ANGELONE": "Fin Service", "BAJFINANCE": "Fin Service",
    "BAJAJFINSV": "Fin Service", "BAJAJHLDNG": "Fin Service", "BSE": "Fin Service", 
    "CDSL": "Fin Service", "CHOLAFIN": "Fin Service", "HDFCAMC": "Fin Service", "HDFCLIFE": "Fin Service",
    "ICICIGI": "Fin Service", "ICICIPRULI": "Fin Service", "IRFC": "Fin Service", "JIOFIN": "Fin Service",
    "KFINTECH": "Fin Service", "LICI": "Fin Service", "LTF": "Fin Service", "LICHSGFIN": "Fin Service",
    "MANAPPURAM": "Fin Service", "MFSL": "Fin Service", "MOTILALOFS": "Fin Service", "MCX": "Fin Service",
    "MUTHOOTFIN": "Fin Service", "NAM-INDIA": "Fin Service", "PAYTM": "Fin Service", "POLICYBZR": "Fin Service",
    "PNBHOUSING": "Fin Service", "PFC": "Fin Service", "RECLTD": "Fin Service", "SBICARD": "Fin Service",
    "SBILIFE": "Fin Service", "SHRIRAMFIN": "Fin Service","BRITANNIA": "Nifty FMCG", "COLPAL": "Nifty FMCG", "DABUR": "Nifty FMCG", "DMART": "Nifty FMCG",
    "GODFRYPHLP": "Nifty FMCG", "GODREJCP": "Nifty FMCG", "HINDUNILVR": "Nifty FMCG", "ITC": "Nifty FMCG",
    "JUBLFOOD": "Nifty FMCG", "MARICO": "Nifty FMCG", "NESTLEIND": "Nifty FMCG", "NYKAA": "Nifty FMCG",
    "PATANJALI": "Nifty FMCG", "RADICO": "Nifty FMCG", "SWIGGY": "Nifty FMCG", "TATACONSUM": "Nifty FMCG",
    "TRENT": "Nifty FMCG", "UNITDSPR": "Nifty FMCG", "VBL": "Nifty FMCG", "VISHAL": "Nifty FMCG",
    "KALYANKJIL": "Nifty FMCG", "TITAN": "Nifty FMCG", 
    "ADANIPORTS": "Realty & Infra", "DLF": "Realty & Infra", "GMRINFRA": "Realty & Infra", "GODREJPROP": "Realty & Infra",
    "LODHA": "Realty & Infra", "NBCC": "Realty & Infra", "OBEROIRLTY": "Realty & Infra", "PHOENIXLTD": "Realty & Infra",
    "PRESTIGE": "Realty & Infra", "RVNL": "Realty & Infra",
    "AMBUJACEM": "Cement & Chem", "GRASIM": "Cement & Chem", "SHREECEM": "Cement & Chem", "ULTRACEMCO": "Cement & Chem",
    "PIIND": "Cement & Chem", "SRF": "Cement & Chem", "UPL": "Cement & Chem",
    "ABB": "Cap Goods & Def", "ADANIENT": "Cap Goods & Def", "AMBER": "Cap Goods & Def", "ASTRAL": "Cap Goods & Def",
    "BDL": "Cap Goods & Def", "BEL": "Cap Goods & Def", "BHARATFORG": "Cap Goods & Def", "BLUESTARCO": "Cap Goods & Def",
    "COCHINSHIP": "Cap Goods & Def", "CROMPTON": "Cap Goods & Def", "CUMMINSIND": "Cap Goods & Def",
    "DIXON": "Cap Goods & Def", "HAL": "Cap Goods & Def", "HAVELLS": "Cap Goods & Def", "HITACHI": "Cap Goods & Def",
    "KAYNES": "Cap Goods & Def", "KEI": "Cap Goods & Def", "LT": "Cap Goods & Def", "MAZDOCK": "Cap Goods & Def",
    "PGEL": "Cap Goods & Def", "PIDILITIND": "Cap Goods & Def", "POLYCAB": "Cap Goods & Def", "SIEMENS": "Cap Goods & Def",
    "SOLARINDS": "Cap Goods & Def", "SUPREMEIND": "Cap Goods & Def", "VOLTAS": "Cap Goods & Def",
    "ATHER": "Telecom & Logistics", "BHARTIARTL": "Telecom & Logistics", "CONCOR": "Telecom & Logistics",
    "DELHIVERY": "Telecom & Logistics", "ETERNAL": "Telecom & Logistics", "IDEA": "Telecom & Logistics",
    "INDIGO": "Telecom & Logistics", "INDUSTOWER": "Telecom & Logistics", "NAUKRI": "Telecom & Logistics",
    "PAGEIND": "Telecom & Logistics", "SAGILITY": "Telecom & Logistics", "INDHOTEL": "Telecom & Logistics"
}

ALL_NSE_EQ_MAP = {}

def load_all_upstox_instruments():
    global ALL_NSE_EQ_MAP
    if ALL_NSE_EQ_MAP: return ALL_NSE_EQ_MAP
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=15) as response:
            with gzip.open(response, 'rt', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) < 4: continue
                    inst_key, symbol, name = row[0].strip(), row[2].strip(), row[3].strip()
                    if inst_key.startswith("NSE_EQ|") and symbol not in ALL_NSE_EQ_MAP:
                        ALL_NSE_EQ_MAP[symbol] = {"inst_key": inst_key, "name": name}
    except Exception as e:
        print("Error loading Upstox instruments:", e)
    return ALL_NSE_EQ_MAP

def load_dynamic_universe():
    inst_map = load_all_upstox_instruments()
    universe = []
    for symbol, sector in TARGET_FO_SYMBOLS.items():
        if symbol in inst_map:
            universe.append({"symbol": symbol, "name": inst_map[symbol]["name"], "sector": sector, "isin": inst_map[symbol]["inst_key"], "is_fo": True})
    return universe if universe else [{"symbol": "NIFTY", "name": "Fallback", "sector": "Error", "isin": "NSE_INDEX|Nifty 50", "is_fo": False}]

GLOBAL_MARKET_UNIVERSE = load_dynamic_universe()
CORE_MARKET_UNIVERSE = GLOBAL_MARKET_UNIVERSE.copy()
NIFTY500_UNIVERSE_CACHE = []

def get_nifty_500_data():
    urls = ['https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv', 'https://archives.nseindia.com/content/indices/ind_nifty500list.csv']
    headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'text/csv', 'Referer': 'https://www.nseindia.com/'}
    for url in urls:
        try:
            res = HTTP_SESSION.get(url, headers=headers, timeout=8)
            if res.status_code == 200 and "Symbol" in res.text:
                lines = res.text.strip().splitlines()
                reader = csv.reader(lines)
                next(reader, None)
                return {row[2].strip(): row[1].strip() for row in reader if len(row) > 2}
        except Exception:
            pass
    return {}

def load_nifty500_universe():
    global NIFTY500_UNIVERSE_CACHE
    if NIFTY500_UNIVERSE_CACHE: return NIFTY500_UNIVERSE_CACHE
    inst_map = load_all_upstox_instruments()
    nifty_data = get_nifty_500_data()
    universe, seen = [], set()
    if nifty_data:
        for symbol, nse_sector in nifty_data.items():
            if symbol in inst_map and symbol not in seen:
                seen.add(symbol)
                universe.append({"symbol": symbol, "name": inst_map[symbol]["name"], "sector": TARGET_FO_SYMBOLS.get(symbol) or nse_sector or "NIFTY 500", "isin": inst_map[symbol]["inst_key"], "is_fo": symbol in TARGET_FO_SYMBOLS})
    if not universe:
        for symbol, inst in inst_map.items():
            if symbol not in seen:
                seen.add(symbol)
                universe.append({"symbol": symbol, "name": inst["name"], "sector": TARGET_FO_SYMBOLS.get(symbol, "General"), "isin": inst["inst_key"], "is_fo": symbol in TARGET_FO_SYMBOLS})
    NIFTY500_UNIVERSE_CACHE = universe[:400]
    return NIFTY500_UNIVERSE_CACHE


def get_active_universe_list(univ_type=None):
    u = (univ_type or request.args.get("universe", "FO")).upper()
    if u == "NIFTY500": return ("NIFTY500", load_nifty500_universe())
    return ("FO", GLOBAL_MARKET_UNIVERSE)

def get_index_csv_data(url):
    headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'text/csv', 'Referer': 'https://www.nseindia.com/'}
    try:
        res = HTTP_SESSION.get(url, headers=headers, timeout=8)
        if res.status_code == 200 and "Symbol" in res.text:
            lines = res.text.strip().splitlines()
            reader = csv.reader(lines)
            next(reader, None)
            return {row[2].strip(): row[1].strip() for row in reader if len(row) > 2}
    except Exception:
        pass
    return {}

def load_nifty500_universe():
    global NIFTY500_UNIVERSE_CACHE
    if NIFTY500_UNIVERSE_CACHE: return NIFTY500_UNIVERSE_CACHE
    inst_map = load_all_upstox_instruments()
    nifty_data = get_nifty_500_data()
    universe, seen = [], set()
    if nifty_data:
        for symbol, nse_sector in nifty_data.items():
            if symbol in inst_map and symbol not in seen:
                seen.add(symbol)
                universe.append({"symbol": symbol, "name": inst_map[symbol]["name"], "sector": TARGET_FO_SYMBOLS.get(symbol) or nse_sector or "NIFTY 500", "isin": inst_map[symbol]["inst_key"], "is_fo": symbol in TARGET_FO_SYMBOLS})
    if not universe:
        for symbol, inst in inst_map.items():
            if symbol not in seen:
                seen.add(symbol)
                universe.append({"symbol": symbol, "name": inst["name"], "sector": TARGET_FO_SYMBOLS.get(symbol, "General"), "isin": inst["inst_key"], "is_fo": symbol in TARGET_FO_SYMBOLS})
    NIFTY500_UNIVERSE_CACHE = universe[:400]
    return NIFTY500_UNIVERSE_CACHE

def load_midcap_universe():
    global MIDCAP_UNIVERSE_CACHE
    if MIDCAP_UNIVERSE_CACHE: return MIDCAP_UNIVERSE_CACHE
    inst_map = load_all_upstox_instruments()
    data = get_index_csv_data('https://nsearchives.nseindia.com/content/indices/ind_niftymidcap150list.csv')
    universe, seen = [], set()
    if data:
        for symbol, nse_sector in data.items():
            if symbol in inst_map and symbol not in seen:
                seen.add(symbol)
                universe.append({"symbol": symbol, "name": inst_map[symbol]["name"], "sector": nse_sector or "Midcap", "isin": inst_map[symbol]["inst_key"], "is_fo": symbol in TARGET_FO_SYMBOLS})
    if not universe:
        for symbol, inst in inst_map.items():
            if symbol not in TARGET_FO_SYMBOLS and symbol not in seen:
                seen.add(symbol)
                universe.append({"symbol": symbol, "name": inst["name"], "sector": "Midcap", "isin": inst["inst_key"], "is_fo": False})
                if len(universe) >= 150: break
    MIDCAP_UNIVERSE_CACHE = universe
    return MIDCAP_UNIVERSE_CACHE

def load_smallcap_universe():
    global SMALLCAP_UNIVERSE_CACHE
    if SMALLCAP_UNIVERSE_CACHE: return SMALLCAP_UNIVERSE_CACHE
    inst_map = load_all_upstox_instruments()
    data = get_index_csv_data('https://nsearchives.nseindia.com/content/indices/ind_niftysmallcap250list.csv')
    universe, seen = [], set()
    if data:
        for symbol, nse_sector in data.items():
            if symbol in inst_map and symbol not in seen:
                seen.add(symbol)
                universe.append({"symbol": symbol, "name": inst_map[symbol]["name"], "sector": nse_sector or "Smallcap", "isin": inst_map[symbol]["inst_key"], "is_fo": symbol in TARGET_FO_SYMBOLS})
    if not universe:
        sorted_insts = list(inst_map.items())
        for symbol, inst in sorted_insts[150:350]:
            if symbol not in seen:
                seen.add(symbol)
                universe.append({"symbol": symbol, "name": inst["name"], "sector": "Smallcap", "isin": inst["inst_key"], "is_fo": False})
    SMALLCAP_UNIVERSE_CACHE = universe
    return SMALLCAP_UNIVERSE_CACHE

def load_total_market_universe():
    global TOTAL_MARKET_UNIVERSE_CACHE
    if TOTAL_MARKET_UNIVERSE_CACHE: return TOTAL_MARKET_UNIVERSE_CACHE
    inst_map = load_all_upstox_instruments()
    universe, seen = [], set()
    for symbol, inst in inst_map.items():
        if symbol not in seen:
            seen.add(symbol)
            universe.append({"symbol": symbol, "name": inst["name"], "sector": TARGET_FO_SYMBOLS.get(symbol, "Total Market"), "isin": inst["inst_key"], "is_fo": symbol in TARGET_FO_SYMBOLS})
    TOTAL_MARKET_UNIVERSE_CACHE = universe[:500]
    return TOTAL_MARKET_UNIVERSE_CACHE

def get_active_universe_list(univ_type=None):
    u = (univ_type or request.args.get("universe", "FO")).upper()
    if u == "NIFTY500": return ("NIFTY500", load_nifty500_universe())
    elif u == "MIDCAP": return ("MIDCAP", load_midcap_universe())
    elif u == "SMALLCAP": return ("SMALLCAP", load_smallcap_universe())
    elif u == "TOTALMARKET": return ("TOTALMARKET", load_total_market_universe())
    return ("FO", GLOBAL_MARKET_UNIVERSE)

def detect_open_setup(open_p, high_p, low_p):
    if open_p <= 0 or low_p <= 0 or high_p <= 0: return None
    tolerance = min(0.03, open_p * 0.00005)
    if abs(open_p - low_p) <= tolerance: 
        return "OPEN=LOW"
    elif abs(open_p - high_p) <= tolerance: 
        return "OPEN=HIGH"
    return None

def calculate_oi_buildup(price_pct, oi_pct, is_fo, candle_tag):
    if not is_fo:
        if candle_tag == "OPEN=LOW" or price_pct >= 2.5: return "Long Buildup"
        elif candle_tag == "OPEN=HIGH" or price_pct <= -2.5: return "Short Buildup"
        elif price_pct > 0: return "Short Covering"
        elif price_pct < 0: return "Long Unwinding"
        return "Neutral"
    if candle_tag == "OPEN=HIGH" or price_pct <= -0.3: return "Short Buildup"
    elif candle_tag == "OPEN=LOW" or price_pct >= 0.3: return "Long Buildup"
    elif price_pct > 0: return "Short Covering"
    elif price_pct < 0: return "Long Unwinding"
    return "Neutral"

def compute_aggressive_momentum(price_pct, vol_spike, candle_tag):
    score = (abs(price_pct) * 15.0) + (vol_spike * 10.0)
    if candle_tag in ["OPEN=LOW", "OPEN=HIGH"]: score += 35.0
    return round(score, 1)

def generate_sparkline_data(ltp, price_pct):
    base = ltp / (1 + (price_pct / 100)) if price_pct != -100 else ltp * 0.95
    points = [round(base, 2)]
    for _ in range(14):
        change_step = random.uniform(-0.003, 0.003) * base
        base += change_step
        points.append(round(base, 2))
    points[-1] = ltp
    return points

def estimate_delivery_percentage(volume, avg_volume, price_pct):
    if avg_volume <= 0: return 50.0
    vol_ratio = volume / avg_volume
    if vol_ratio > 2.0 and abs(price_pct) < 2.0: base_delivery = 75.0
    elif vol_ratio > 1.5: base_delivery = 65.0
    else: base_delivery = 45.0
    return round(base_delivery, 1)

def fetch_live_upstox_quotes(univ_type="FO"):
    global QUOTE_CACHE
    if not UPSTOX_ACCESS_TOKEN or UPSTOX_ACCESS_TOKEN == "YOUR_NEW_TOKEN_HERE":
        return {"error": "Invalid API Token."}
    univ_key, target_universe = get_active_universe_list(univ_type)
    cache_entry = QUOTE_CACHE.setdefault(univ_key, {"data": [], "last_updated": 0})
    if time.time() - cache_entry["last_updated"] < 5 and cache_entry["data"]:
        return cache_entry["data"]

    results, chunk_size = [], 100
    headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}

    def fetch_chunk(chunk):
        chunk_results = []
        keys_param = ",".join([item["isin"] for item in chunk])
        url = "https://api.upstox.com/v2/market-quote/quotes"
        try:
            res = HTTP_SESSION.get(url, headers=headers, params={"instrument_key": keys_param}, timeout=8)
            if res.status_code == 401: return {"error": "Upstox API Token Expired."}
            elif res.status_code != 200: return []
            quotes_data = res.json().get("data", {})
            for item in chunk:
                sym, isin_raw = item["symbol"], item["isin"]
                q = quotes_data.get(isin_raw) or quotes_data.get(isin_raw.replace("|", ":")) or quotes_data.get(f"NSE_EQ:{sym}") or quotes_data.get(sym)
                if not q: continue
                ltp = float(q.get("last_price", 0.0))
                ohlc = q.get("ohlc", {})
                open_p, low_p, high_p, volume = float(ohlc.get("open") or 0.0), float(ohlc.get("low") or 0.0), float(ohlc.get("high") or 0.0), int(q.get("volume", 0))
                average_volume = int(q.get("average_volume") or q.get("avg_volume") or (volume * 0.6) or 100000)
                net_change, prev_close = float(q.get("net_change") or 0.0), float(q.get("prev_close") or q.get("prev_close_price") or 0.0)
                if prev_close == 0.0 and (ltp - net_change) > 0: prev_close = ltp - net_change
                price_pct = round(((ltp - prev_close) / prev_close) * 100, 2) if prev_close > 0 else 0.0
                candle_tag = detect_open_setup(open_p, high_p, low_p)
                oi, prev_oi = float(q.get("oi") or 0.0), float(q.get("prev_oi") or 0.0)
                oi_pct = round(((oi - prev_oi) / prev_oi) * 100, 2) if (oi > 0 and prev_oi > 0) else round(abs(price_pct) * 1.25, 2)
                vol_spike = volume / average_volume if average_volume > 0 else 1.0
                
                delivery_pct = estimate_delivery_percentage(volume, average_volume, price_pct)
                is_heavy_delivery = delivery_pct >= 60.0

                tv_url = f"https://www.tradingview.com/chart/?symbol=NSE:{sym}"
                groww_url = f"https://groww.in/stocks/{sym.lower()}"
                sparkline_points = generate_sparkline_data(ltp, price_pct)

                chunk_results.append({
                    "symbol": sym, "name": item["name"], "sector": item.get("sector", "NIFTY 500"),
                    "ltp": ltp, "pricePct": price_pct, "volume": volume, "volMultiplier": round(vol_spike, 2),
                    "oiPct": oi_pct, "buildup": calculate_oi_buildup(price_pct, oi_pct, item.get("is_fo", True), candle_tag),
                    "momentumScore": compute_aggressive_momentum(price_pct, vol_spike, candle_tag),
                    "candleTag": candle_tag, "is_fo": item.get("is_fo", True),
                    "deliveryPct": delivery_pct, "isHeavyDelivery": is_heavy_delivery,
                    "tvUrl": tv_url, "growwUrl": groww_url, "sparkline": sparkline_points
                })
        except Exception:
            pass
        return chunk_results

    chunks = [target_universe[i:i + chunk_size] for i in range(0, len(target_universe), chunk_size)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(fetch_chunk, c) for c in chunks]
        for f in concurrent.futures.as_completed(futures):
            res = f.result()
            if isinstance(res, dict) and "error" in res: return res
            if isinstance(res, list): results.extend(res)

    if results:
        results.sort(key=lambda x: x["momentumScore"], reverse=True)
        cache_entry["data"], cache_entry["last_updated"] = results, time.time()
        return results
    return {"error": "No market data retrieved."}

# ================= BACKGROUND THREADS FOR SMART ORB =================
def evaluate_smart_orb_strategy():
    global SMART_ORB_SIGNALS
    while True:
        now = datetime.now(pytz.timezone("Asia/Kolkata"))
        if now.weekday() < 5 and now.hour == 9 and now.minute == 20:
            try:
                stocks = fetch_live_upstox_quotes("FO")
                if isinstance(stocks, list) and len(stocks) > 0:
                    advances = sum(1 for s in stocks if s.get('pricePct', 0) > 0)
                    declines = sum(1 for s in stocks if s.get('pricePct', 0) < 0)
                    market_is_bullish = advances >= declines
                    scored_stocks = []
                    for stock in stocks:
                        score, bias = 0, "NEUTRAL"
                        price_pct, buildup, candle_tag, vol_multiplier = stock.get('pricePct', 0), stock.get('buildup', ''), stock.get('candleTag', ''), stock.get('volMultiplier', 1.0)
                        if market_is_bullish:
                            if candle_tag == 'OPEN=LOW': score += 40
                            if buildup in ['Long Buildup', 'Short Covering']: score += 30
                            if vol_multiplier >= 1.1: score += 20
                            if price_pct > 0: score += 10
                            if score >= 10: bias = "BULLISH_ORB"
                        else:
                            if candle_tag == 'OPEN=HIGH': score += 40
                            if buildup in ['Short Buildup', 'Long Unwinding']: score += 30
                            if vol_multiplier >= 1.1: score += 20
                            if price_pct < 0: score += 10
                            if score >= 10: bias = "BEARISH_ORB"
                        if bias != "NEUTRAL":
                            scored_stocks.append({
                                "symbol": stock["symbol"], "bias": bias, "score": score,
                                "ltp": stock["ltp"], "sector": stock["sector"],
                                "reason": f"Score: {score} | Tag: {candle_tag or 'N/A'} | Buildup: {buildup or 'N/A'} | Vol: {vol_multiplier}x"
                            })
                    scored_stocks.sort(key=lambda x: x['score'], reverse=True)
                    SMART_ORB_SIGNALS = scored_stocks[:2]
            except Exception as e:
                print("Error in Smart ORB background task:", e)
            time.sleep(70)
        time.sleep(10)

def start_smart_orb_background_thread():
    t = threading.Thread(target=evaluate_smart_orb_strategy, daemon=True)
    t.start()

# ================= HIGHER TIMEFRAME AGGREGATION & ZONE SCANNER =================
def aggregate_candles(candles, timeframe):
    if not candles: return []
    df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('date', inplace=True)
    
    rule = '3ME' if timeframe == 'quarter' else ('6ME' if timeframe == 'halfyear' else '1YE')
    
    agg_dict = {
        'timestamp': 'first',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    
    resampled = df.resample(rule).agg(agg_dict).dropna()
    return resampled[['timestamp', 'open', 'high', 'low', 'close', 'volume']].values.tolist()

def scan_four_institutional_zones(candles, current_ltp, symbol, tf, max_dist_pct=None):
    zones = []
    if len(candles) < 12: return zones

    # Strictly limit base candles to 1 or 2 (NO 3 base candles allowed)
    for num_bases in [1, 2]:
        for i in range(num_bases, len(candles) - 3):
            legin_idx = i - num_bases
            base_start_idx = i
            base_end_idx = i + num_bases - 1
            legout_idx = base_end_idx + 1

            if legin_idx < 0 or legout_idx >= len(candles): continue

            legin = candles[legin_idx]
            legout = candles[legout_idx]
            base_candles = candles[base_start_idx : base_end_idx + 1]

            if len(base_candles) > 2: continue

            legin_o, legin_c, legin_h, legin_l = legin[1], legin[4], legin[2], legin[3]
            legout_o, legout_c, legout_h, legout_l = legout[1], legout[4], legout[2], legout[3]

            legin_body = abs(legin_c - legin_o)
            legin_range = legin_h - legin_l
            legout_body = abs(legout_c - legout_o)
            legout_range = legout_h - legout_l

            if legin_range <= 0 or legout_range <= 0: continue

            # 1. Leg-in Rule: Body must be 70% to 98% of total range
            legin_body_ratio = legin_body / legin_range
            if not (0.70 <= legin_body_ratio <= 0.98):
                continue

            # 2. Leg-out Rule: Body must be strong (75% to 98%) and stronger than leg-in
            legout_body_ratio = legout_body / legout_range
            if not (0.75 <= legout_body_ratio <= 0.98) or legout_body < legin_body:
                continue

            # 3. Base Candle Rule: Must be Doji / compact base (< 30% of range)
            all_bases_valid = True
            for bc in base_candles:
                bc_o, bc_c, bc_h, bc_l = bc[1], bc[4], bc[2], bc[3]
                bc_range = bc_h - bc_l
                bc_body = abs(bc_c - bc_o)
                
                if bc_range <= 0:
                    all_bases_valid = False
                    break
                
                bc_body_ratio = bc_body / bc_range
                if bc_body_ratio > 0.30 or bc_body > (legin_body * 0.35):
                    all_bases_valid = False
                    break

            if not all_bases_valid: continue

            is_legin_rally = legin_c > legin_o
            is_legout_rally = legout_c > legout_o

            pattern, bias, proximal, distal = None, None, 0.0, 0.0

            if is_legout_rally:
                bias = "DEMAND"
                pattern = "Rally-Base-Rally" if is_legin_rally else "Drop-Base-Rally"
                proximal = max(max(bc[1], bc[4]) for bc in base_candles)
                distal = min(bc[3] for bc in base_candles)
            else:
                bias = "SUPPLY"
                pattern = "Rally-Base-Drop" if is_legin_rally else "Drop-Base-Drop"
                proximal = min(min(bc[1], bc[4]) for bc in base_candles)
                distal = max(bc[2] for bc in base_candles)

            # 4. Calculate Distance from CMP (%)
            distance_pct = round(abs(current_ltp - proximal) / current_ltp * 100, 2)
            if max_dist_pct is not None and distance_pct > max_dist_pct:
                continue

            # 5. Violation & Test Count Check: Discard if violated or tested > 1 time
            is_violated = False
            test_count = 0
            for sc in candles[legout_idx + 1:]:
                sc_high, sc_low, sc_close = sc[2], sc[3], sc[4]
                if bias == "DEMAND":
                    if sc_low <= proximal and sc_high >= distal:
                        test_count += 1
                    if sc_close < distal:
                        is_violated = True
                        break
                else:
                    if sc_high >= proximal and sc_low <= distal:
                        test_count += 1
                    if sc_close > distal:
                        is_violated = True
                        break

            if is_violated: continue
            if test_count > 1: continue

            risk = round(abs(proximal - distal), 2)
            if risk <= 0: continue
            risk_pct = round((risk / current_ltp) * 100, 2) if current_ltp > 0 else 0

            status = "Untested (Fresh)" if test_count == 0 else f"Tested ({test_count}x)"
            if bias == "DEMAND":
                if distal <= current_ltp <= proximal: status = "🎯 In Zone (Actionable)"
                elif current_ltp < distal: continue
            else:
                if proximal <= current_ltp <= distal: status = "🎯 In Zone (Actionable)"
                elif current_ltp > distal: continue

            zones.append({
                "pattern": pattern, "bias": bias, "proximal": round(proximal, 2), "distal": round(distal, 2),
                "entry": proximal, "stopLoss": distal, "risk": risk, "riskPct": risk_pct,
                "target1R2": round(proximal + (risk * 2.0) if bias == 'DEMAND' else proximal - (risk * 2.0), 2),
                "target1R3": round(proximal + (risk * 3.0) if bias == 'DEMAND' else proximal - (risk * 3.0), 2),
                "status": status, "distancePct": distance_pct, "multiplier": round(legout_body / max(legin_body, 0.01), 2),
                "tvUrl": f"https://www.tradingview.com/chart/?symbol=NSE:{symbol}",
                "brokerUrl": f"https://groww.in/stocks/{symbol.lower()}",
                "timeframe": tf.upper()
            })
    return zones

def fetch_historical_candles(instrument_key, interval):
    global CANDLE_CACHE
    api_interval = interval
    is_custom_tf = False

    if interval in ["quarter", "halfyear", "year"]:
        api_interval = "month"
        is_custom_tf = True

    cache_key = f"{instrument_key}_{interval}"
    with CANDLE_CACHE_LOCK:
        cached = CANDLE_CACHE.get(cache_key)
    if cached and (time.time() - cached["ts"] < 3600): return cached["candles"]

    to_date = date.today()
    days_back = 3650 if is_custom_tf else (1825 if interval in ["week", "month"] else 300)
    from_date = to_date - timedelta(days=days_back)
    
    url = f"https://api.upstox.com/v2/historical-candle/{urllib.parse.quote(instrument_key, safe='')}/{api_interval}/{to_date.strftime('%Y-%m-%d')}/{from_date.strftime('%Y-%m-%d')}"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}

    for attempt in range(3):
        try:
            res = HTTP_SESSION.get(url, headers=headers, timeout=4)
            if res.status_code == 200:
                candles = res.json().get("data", {}).get("candles", [])
                candles.reverse()
                if is_custom_tf:
                    candles = aggregate_candles(candles, interval)
                with CANDLE_CACHE_LOCK:
                    CANDLE_CACHE[cache_key] = {"candles": candles, "ts": time.time()}
                return candles
            elif res.status_code == 429: time.sleep(0.3 * (attempt + 1))
        except Exception:
            time.sleep(0.2)
    return []

# ================= GMAIL / RESEND SMTP EMAIL DISPATCH =================
import resend
resend.api_key = os.getenv("RESEND_API_KEY", "re_PXnppuZw_3EHdCanGDBsMMyETMGQWB5wS")

def send_otp_email(to_email, otp_code):
    try:
        params = {
            "from": "Stock Finder Pro <onboarding@resend.dev>", 
            "to": [to_email],
            "subject": "Password Reset OTP - Stock Finder Pro",
            "html": f"""
                <div style="font-family: Arial, sans-serif; color: #333;">
                    <h2>Password Reset Request</h2>
                    <p>Your password reset verification code is:</p>
                    <h1 style="color: #f39c12; letter-spacing: 4px;">{otp_code}</h1>
                    <p>This code is valid for 10 minutes.</p>
                </div>
            """
        }
        resend.Emails.send(params)
        print(f"✅ OTP email successfully dispatched via Resend to {to_email}")
        return True
    except Exception as e:
        print(f"❌ Failed to send email via Resend API: {e}")
        return False

# ================= HTML TEMPLATES WITH STANDARD FORM POST =================
LANDING_PAGE_HTML = '<!DOCTYPE html>\n<html lang="en">\n<head>\n<meta charset="UTF-8">\n<meta name="viewport" content="width=device-width, initial-scale=1.0">\n<meta name="theme-color" content="#07090d">\n<meta name="description" content="Stock Finder Pro — institutional-style intraday stock scanning and price action analysis.">\n<title>Stock Finder Pro | Trade with an Edge</title>\n<style>\n:root{--bg:#07090d;--panel:#10141b;--line:#252c36;--text:#f7f8fb;--muted:#929ba9;--orange:#ff9e0b;--green:#22d982;--red:#ff5364;--blue:#42a5ff}\n*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:var(--bg);color:var(--text);font-family:Inter,ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,Arial,sans-serif;overflow-x:hidden}a{color:inherit;text-decoration:none}\n#market-bg{position:fixed;inset:0;width:100%;height:100%;z-index:-3;opacity:.72;pointer-events:none}\n.bg-vignette{position:fixed;inset:0;z-index:-2;pointer-events:none;background:radial-gradient(ellipse at 50% 34%,transparent 0%,rgba(7,9,13,.28) 45%,#07090d 100%),linear-gradient(180deg,rgba(7,9,13,.15),#07090d 92%)}\n.scanline{position:fixed;inset:0;z-index:-1;pointer-events:none;opacity:.12;background:repeating-linear-gradient(0deg,transparent 0 3px,rgba(255,255,255,.025) 4px)}\n.wrap{width:min(1200px,calc(100% - 48px));margin:auto}\nheader{height:76px;border-bottom:1px solid #ffffff13;background:rgba(7,9,13,.72);display:flex;align-items:center;justify-content:space-between;padding:0 max(24px,calc((100vw - 1690px)/2));position:sticky;top:0;z-index:10;backdrop-filter:blur(18px)}\n.brand{display:flex;align-items:center;gap:10px;font-size:18px;font-weight:900;letter-spacing:-.55px;color:var(--orange)}.brand-dot{height:9px;width:9px;border-radius:50%;background:var(--green);box-shadow:0 0 8px var(--green),0 0 20px #22d98288;animation:pulse 1.8s infinite}\n.nav-actions{display:flex;gap:10px;align-items:center}.btn{display:inline-flex;align-items:center;justify-content:center;min-height:44px;padding:0 20px;border-radius:10px;font-size:13px;font-weight:800;border:1px solid transparent;transition:transform .2s,box-shadow .2s,border-color .2s;cursor:pointer}.btn:hover{transform:translateY(-3px)}.btn-dark{background:#11151c;border-color:#303743;box-shadow:0 5px 18px #0008}.btn-orange{background:linear-gradient(110deg,#ffb21a,#ff8b00);color:#111;box-shadow:0 0 25px #ff990033,0 8px 28px #0008}.btn-orange:hover{box-shadow:0 0 34px #ff990066}\n.hero{position:relative;text-align:center;padding:88px 20px 76px;min-height:730px;display:flex;flex-direction:column;align-items:center;isolation:isolate}\n.hero:before{content:"";position:absolute;width:min(850px,90vw);height:470px;top:40px;left:50%;transform:translateX(-50%);z-index:-1;background:radial-gradient(ellipse,#ff990b12 0%,#ff990b05 38%,transparent 70%);filter:blur(18px);animation:glow 6s ease-in-out infinite alternate}\n.eyebrow{display:inline-flex;align-items:center;gap:8px;padding:6px 14px;border:1px solid #a5671c88;background:#21170b99;color:#ffb13b;border-radius:30px;font-size:10px;font-weight:900;letter-spacing:.65px;margin-bottom:25px;box-shadow:0 0 25px #ff9d0a12}.live-dot{width:6px;height:6px;background:var(--green);border-radius:50%;box-shadow:0 0 9px var(--green)}\nh1{font-size:clamp(40px,5vw,62px);line-height:1.08;letter-spacing:-2.8px;margin:0 auto 22px;max-width:850px;font-weight:900;text-shadow:0 4px 45px #000} .accent{color:var(--orange);text-shadow:0 0 35px #ff9d0a30}\n.hero-copy{max-width:610px;color:#a3adbb;font-size:15px;line-height:1.8;margin:0 auto 32px}.hero-actions{display:flex;gap:13px;justify-content:center;flex-wrap:wrap}.hero-actions .btn{min-width:190px;min-height:48px}\n.market-tape{display:flex;gap:10px;justify-content:center;flex-wrap:wrap;margin-top:32px}.ticker{border:1px solid #ffffff18;background:#0c1118b8;border-radius:8px;padding:7px 12px;font:700 10px ui-monospace,monospace;color:#b5bfcd;backdrop-filter:blur(8px)}.up{color:var(--green)}.down{color:var(--red)}\n.terminal{width:min(900px,100%);margin-top:75px;background:linear-gradient(145deg,#151b24eF,#0c1017ed);border:1px solid #ffffff20;border-radius:16px;padding:13px;box-shadow:0 25px 75px #000b,0 0 45px #ff9d0a0b;text-align:left;position:relative;overflow:hidden;backdrop-filter:blur(12px)}\n.terminal:after{content:"";position:absolute;top:-120%;left:-40%;width:25%;height:340%;transform:rotate(28deg);background:linear-gradient(90deg,transparent,#ffffff0c,transparent);animation:shine 8s ease-in-out infinite}\n.terminal-top{height:23px;display:flex;align-items:flex-start;gap:5px;padding-left:7px}.light{width:9px;height:9px;border-radius:50%}.light.red{background:#ff514a}.light.yellow{background:#ffbd2e}.light.green{background:#28c840}\n.terminal-screen{min-height:78px;border:1px solid #ffffff12;background:#090d13c9;border-radius:10px;display:flex;align-items:center;justify-content:center;text-align:center;padding:16px;color:#9ba5b4;font:600 12px ui-monospace,SFMono-Regular,Consolas,monospace;letter-spacing:.1px}.terminal-screen b{color:var(--orange);margin-right:9px;font-size:15px}\n.status{display:flex;justify-content:center;gap:18px;flex-wrap:wrap;margin-top:14px;color:#7f8a99;font:10px ui-monospace,monospace}.status span:before{content:"";display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--green);margin:0 7px 1px 0;box-shadow:0 0 8px var(--green)}\nsection.content{padding:78px 0;border-top:1px solid #ffffff12;background:linear-gradient(180deg,#090d13b5,#080b10eF)}\n.section-head{text-align:center;max-width:680px;margin:0 auto 38px}.kicker{color:var(--orange);font-size:10px;font-weight:900;letter-spacing:1.8px;text-transform:uppercase}.section-head h2{font-size:clamp(28px,3.5vw,40px);line-height:1.18;letter-spacing:-1.3px;margin:10px 0}.section-head p{color:var(--muted);font-size:14px;margin:0}\n.cards{display:grid;grid-template-columns:repeat(3,1fr);gap:16px}.card{padding:25px;border:1px solid #ffffff19;border-radius:14px;background:linear-gradient(145deg,#151c27df,#0c1119e8);backdrop-filter:blur(10px);transition:.25s;position:relative;overflow:hidden}.card:hover{transform:translateY(-5px);border-color:#ff9d0a66;box-shadow:0 18px 45px #0006}.icon{width:43px;height:43px;border-radius:12px;display:grid;place-items:center;background:#2a1d0b;color:var(--orange);font-size:21px;margin-bottom:16px;box-shadow:inset 0 0 18px #ff9d0a14}.card h3{font-size:17px;margin:0 0 8px}.card p{font-size:13px;color:var(--muted);margin:0}\n.steps{display:grid;grid-template-columns:repeat(4,1fr);gap:13px}.step{padding:21px;border:1px solid #ffffff19;border-radius:12px;background:#101720d9;backdrop-filter:blur(8px)}.step small{color:var(--orange);font-weight:900;letter-spacing:1px}.step h3{font-size:15px;margin:8px 0 5px}.step p{font-size:12px;color:var(--muted);margin:0}\n.cta{padding:50px 24px;text-align:center;border:1px solid #a66b2355;border-radius:18px;background:radial-gradient(ellipse at 50% 0%,#ff9d0a20,transparent 65%),#111720eF;box-shadow:0 20px 65px #0007}.cta h2{font-size:clamp(27px,3.5vw,40px);margin:8px 0}.cta p{max-width:560px;color:var(--muted);margin:0 auto 23px;font-size:14px}\n.footer{padding:25px 0;border-top:1px solid #ffffff16;color:#777f8b;font-size:12px;background:#07090d}.footer-row{display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap}.disclaimer{max-width:850px;margin-top:12px;color:#626b78;font-size:11px}\n@keyframes pulse{50%{box-shadow:0 0 3px var(--green),0 0 12px #22d98255;opacity:.7}}@keyframes glow{to{opacity:.55;transform:translateX(-50%) scale(1.12)}}@keyframes shine{0%,60%{left:-45%}100%{left:130%}}\n@media(max-width:760px){header{height:68px;padding:0 18px}.brand{font-size:15px}.btn{min-height:40px;padding:0 13px;font-size:12px}.hero{padding:72px 16px 60px;min-height:auto}.terminal{margin-top:60px}.cards{grid-template-columns:1fr 1fr}.steps{grid-template-columns:1fr 1fr}}\n@media(max-width:520px){.wrap{width:calc(100% - 30px)}header{padding:0 13px}.brand{font-size:13px}.nav-actions{gap:6px}.nav-actions .btn{padding:0 10px}.hero{padding-top:64px}h1{font-size:39px;letter-spacing:-1.8px}.hero-copy{font-size:14px}.hero-actions{width:100%;gap:10px}.hero-actions .btn{width:100%}.market-tape{gap:6px}.ticker{font-size:9px;padding:6px 8px}.terminal{margin-top:48px;padding:10px}.terminal-screen{font-size:10px;line-height:1.6}.cards,.steps{grid-template-columns:1fr}section.content{padding:58px 0}.footer-row{flex-direction:column}}\n@media(prefers-reduced-motion:reduce){*,*:before,*:after{animation-duration:.01ms!important;animation-iteration-count:1!important;scroll-behavior:auto!important}}\n\n.pricing-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;align-items:stretch}\n.price-card{position:relative;padding:25px 20px 22px;border:1px solid #ffffff1c;border-radius:15px;background:linear-gradient(155deg,#171e29ed,#0d121aef);backdrop-filter:blur(12px);display:flex;flex-direction:column;min-height:330px;transition:.25s;overflow:hidden}\n.price-card:hover{transform:translateY(-6px);border-color:#ff9d0a88;box-shadow:0 20px 45px #0008,0 0 24px #ff9d0a12}\n.price-card.featured{border-color:#ff9d0a99;background:linear-gradient(155deg,#2a1d0fee,#111720f5);box-shadow:0 0 32px #ff9d0a12}\n.plan-tag{position:absolute;top:13px;right:13px;padding:4px 8px;border-radius:20px;background:#ff9d0a1c;color:#ffb43d;font-size:9px;font-weight:900;letter-spacing:.5px}\n.plan-name{font-size:16px;font-weight:850;margin:0 0 5px}.plan-duration{font-size:11px;color:#8f9aaa}\n.plan-price{font-size:34px;line-height:1.2;font-weight:900;letter-spacing:-1.5px;margin:22px 0 2px}.plan-price span{font-size:13px;color:#9ba5b4;font-weight:650;letter-spacing:0}\n.plan-monthly{font-size:11px;color:#9ba5b4;min-height:20px}.plan-save{font-size:10px;color:var(--green);font-weight:800;margin-top:9px;min-height:16px}\n.plan-rule{height:1px;background:#ffffff16;margin:18px 0 15px}.plan-points{list-style:none;padding:0;margin:0 0 20px;color:#aeb7c4;font-size:11px;display:grid;gap:9px}.plan-points li:before{content:"✓";color:var(--green);font-weight:900;margin-right:8px}\n.price-card .btn{width:100%;margin-top:auto;min-height:42px}.pricing-note{text-align:center;color:#778291;font-size:11px;margin:22px auto 0;max-width:700px}\n@media(max-width:900px){.pricing-grid{grid-template-columns:repeat(2,1fr)}}\n@media(max-width:520px){.pricing-grid{grid-template-columns:1fr}.price-card{min-height:0}.plan-price{margin-top:15px}}\n\n</style>\n</head>\n<body>\n<canvas id="market-bg" aria-hidden="true"></canvas><div class="bg-vignette"></div><div class="scanline"></div>\n<header>\n  <a class="brand" href="#home"><span class="brand-dot"></span> STOCK FINDER PRO</a>\n  <nav class="nav-actions" aria-label="Account navigation"><a class="btn btn-dark" href="#pricing">Plans</a><a class="btn btn-dark" href="/login">Login</a><a class="btn btn-orange" href="/signup">Sign Up 🚀</a></nav>\n</header>\n<main id="home">\n  <section class="hero">\n    <div class="eyebrow"><span class="live-dot"></span> STRICT INSTITUTIONAL ORDER FLOW SCREENER</div>\n    <h1>Dominate Intraday Setups<br>Like <span class="accent">Smart Money</span></h1>\n    <p class="hero-copy">Instantly detect strict unviolated institutional Demand &amp; Supply zones with high-precision price action.</p>\n    <div class="hero-actions"><a class="btn btn-orange" href="/signup">Get Started Free 🚀</a><a class="btn btn-dark" href="/login">Login to Terminal 🔐</a></div>\n    <div class="market-tape" aria-label="Illustrative market ticker"><span class="ticker">NIFTY <b class="up">▲ 0.82%</b></span><span class="ticker">BANKNIFTY <b class="up">▲ 0.46%</b></span><span class="ticker">RELIANCE <b class="down">▼ 0.21%</b></span><span class="ticker">HDFCBANK <b class="up">▲ 1.14%</b></span></div>\n    <div class="terminal"><div class="terminal-top"><i class="light red"></i><i class="light yellow"></i><i class="light green"></i></div><div class="terminal-screen"><span><b>ϟ</b> Real-Time Intraday Scanner · Advanced Institutional Price Action &amp; Trend Tracker Active</span></div><div class="status"><span>SCANNER ENGINE READY</span><span>PRICE ACTION MONITORING</span><span>MARKET DATA PREVIEW</span></div></div>\n  </section>\n  <section class="content" id="features"><div class="wrap"><div class="section-head"><div class="kicker">Built for focused traders</div><h2>Find high-quality setups. Skip the chart noise.</h2><p>Bring your stock scanning, zone analysis and trend checks into one focused workflow.</p></div><div class="cards">\n    <article class="card"><div class="icon">⌕</div><h3>Intraday Stock Scanner</h3><p>Scan your selected stock universe and quickly shortlist symbols that match your setup conditions.</p></article>\n    <article class="card"><div class="icon">↕</div><h3>Institutional Demand &amp; Supply</h3><p>Identify potential unviolated demand and supply areas to focus your price-action analysis.</p></article>\n    <article class="card"><div class="icon">⌁</div><h3>Trend &amp; Multi-Timeframe View</h3><p>Review trend direction and higher-timeframe context before planning a trade.</p></article>\n  </div></div></section>\n  <section class="content" id="workflow"><div class="wrap"><div class="section-head"><div class="kicker">Your trading workflow</div><h2>Scan smarter. Execute with a plan.</h2><p>Use the scanner to find candidates, then validate every setup with your own rules.</p></div><div class="steps">\n    <article class="step"><small>01 / SCAN</small><h3>Scan the market</h3><p>Find stocks matching your selected scanning conditions.</p></article>\n    <article class="step"><small>02 / IDENTIFY</small><h3>Mark key zones</h3><p>Review potential institutional demand and supply areas.</p></article>\n    <article class="step"><small>03 / CONFIRM</small><h3>Validate price action</h3><p>Check trend, structure and entry confirmation on your chart.</p></article>\n    <article class="step"><small>04 / EXECUTE</small><h3>Manage your risk</h3><p>Define stop-loss and position size before entering a trade.</p></article>\n  </div></div></section>\n\n  <section class="content" id="pricing">\n    <div class="wrap">\n      <div class="section-head">\n        <div class="kicker">Simple, transparent pricing</div>\n        <h2>Choose your trading edge.</h2>\n        <p>Choose a plan that fits your trading journey. Upgrade your access whenever you need.</p>\n      </div>\n      <div class="pricing-grid">\n        <article class="price-card">\n          <h3 class="plan-name">Monthly</h3><div class="plan-duration">1 month access</div>\n          <div class="plan-price">₹499<span> / plan</span></div>\n          <div class="plan-monthly">₹499 per month</div>\n          <div class="plan-save">&nbsp;</div>\n          <div class="plan-rule"></div>\n          <ul class="plan-points"><li>Scanner access for 1 month</li><li>Demand &amp; supply zone tools</li><li>Intraday market analysis</li></ul>\n          <a class="btn btn-dark" href="/checkout?plan=monthly">Choose Monthly</a>\n        </article>\n        <article class="price-card featured">\n          <span class="plan-tag">BEST VALUE</span>\n          <h3 class="plan-name">Quarterly</h3><div class="plan-duration">3 months access</div>\n          <div class="plan-price">₹1,199<span> / plan</span></div>\n          <div class="plan-monthly">About ₹400 per month</div>\n          <div class="plan-save">Save ₹298 · 20% vs monthly</div>\n          <div class="plan-rule"></div>\n          <ul class="plan-points"><li>Scanner access for 3 months</li><li>Demand &amp; supply zone tools</li><li>Intraday market analysis</li></ul>\n          <a class="btn btn-orange" href="/checkout?plan=quarterly">Choose Quarterly</a>\n        </article>\n        <article class="price-card">\n          <h3 class="plan-name">Half-Yearly</h3><div class="plan-duration">6 months access</div>\n          <div class="plan-price">₹2,499<span> / plan</span></div>\n          <div class="plan-monthly">About ₹417 per month</div>\n          <div class="plan-save">Save ₹495 · 17% vs monthly</div>\n          <div class="plan-rule"></div>\n          <ul class="plan-points"><li>Scanner access for 6 months</li><li>Demand &amp; supply zone tools</li><li>Intraday market analysis</li></ul>\n          <a class="btn btn-dark" href="/checkout?plan=halfyearly">Choose Half-Yearly</a>\n        </article>\n        <article class="price-card">\n          <span class="plan-tag">LONG-TERM</span>\n          <h3 class="plan-name">Yearly</h3><div class="plan-duration">12 months access</div>\n          <div class="plan-price">₹4,999<span> / plan</span></div>\n          <div class="plan-monthly">About ₹417 per month</div>\n          <div class="plan-save">Save ₹989 · 17% vs monthly</div>\n          <div class="plan-rule"></div>\n          <ul class="plan-points"><li>Scanner access for 12 months</li><li>Demand &amp; supply zone tools</li><li>Intraday market analysis</li></ul>\n          <a class="btn btn-dark" href="/checkout?plan=yearly">Choose Yearly</a>\n        </article>\n      </div>\n      <p class="pricing-note">Prices are in INR. Selecting a plan opens secure Razorpay checkout after account creation or login. Access activates only after payment verification.</p>\n    </div>\n  </section>\n\n  <section class="content" id="start"><div class="wrap"><div class="cta"><div class="kicker">Ready when you are</div><h2>Make your next scan count.</h2><p>Get a clearer starting point for your intraday analysis with Stock Finder Pro.</p><a class="btn btn-orange" href="/signup">Create Your Free Account 🚀</a></div></div></section>\n</main>\n<footer class="footer"><div class="wrap"><div class="footer-row"><strong style="color:var(--orange)">STOCK FINDER PRO</strong><span>© <span id="year"></span> Stock Finder Pro. All rights reserved.</span></div><p class="disclaimer">Stock Finder Pro is a market-scanning and educational analysis tool, not investment advice. Any ticker values shown in the here are illustrative design elements, not live market quotes. Scanner results do not guarantee price movement or trading outcomes. Always verify setups independently and use appropriate risk management.</p></div></footer>\n<script>\ndocument.getElementById(\'year\').textContent=new Date().getFullYear();\n// Lightweight animated candlestick market backdrop — no external libraries or data feed.\n(()=>{const c=document.getElementById(\'market-bg\'),ctx=c.getContext(\'2d\');let w,h,dpr,candles=[],raf;\nfunction resize(){dpr=Math.min(window.devicePixelRatio||1,2);w=innerWidth;h=innerHeight;c.width=w*dpr;c.height=h*dpr;c.style.width=w+\'px\';c.style.height=h+\'px\';ctx.setTransform(dpr,0,0,dpr,0,0);const count=Math.ceil(w/25)+8;candles=Array.from({length:count},(_,i)=>({x:i*25,y:h*.42+(Math.random()-.5)*h*.35,body:8+Math.random()*19,wick:12+Math.random()*30,up:Math.random()>.48,speed:.12+Math.random()*.25,phase:Math.random()*6.28}));}\nfunction draw(t){ctx.clearRect(0,0,w,h);const grid=34;ctx.strokeStyle=\'rgba(130,150,175,.055)\';ctx.lineWidth=1;for(let x=0;x<w;x+=grid){ctx.beginPath();ctx.moveTo(x,0);ctx.lineTo(x,h);ctx.stroke()}for(let y=0;y<h;y+=grid){ctx.beginPath();ctx.moveTo(0,y);ctx.lineTo(w,y);ctx.stroke()}\n // faint flowing price trace\n ctx.beginPath();for(let x=0;x<=w;x+=5){let y=h*.48+Math.sin(x*.008+t*.00035)*h*.055+Math.sin(x*.021-t*.00022)*h*.018;if(x===0)ctx.moveTo(x,y);else ctx.lineTo(x,y)}ctx.strokeStyle=\'rgba(255,157,10,.19)\';ctx.lineWidth=1.4;ctx.stroke();\n candles.forEach((k,i)=>{let drift=Math.sin(t*.00045+k.phase)*13+Math.sin(t*.00017+i*.7)*8;let y=k.y+drift;let color=k.up?\'34,217,130\':\'255,83,100\';ctx.strokeStyle=`rgba(${color},.38)`;ctx.fillStyle=`rgba(${color},.28)`;ctx.lineWidth=1.2;ctx.beginPath();ctx.moveTo(k.x,y-k.wick/2);ctx.lineTo(k.x,y+k.wick/2);ctx.stroke();ctx.fillRect(k.x-3.5,y-(k.up?k.body:0),7,k.body);});\n // subtle moving scan beam\n let beam=(t*.035)%(w+300)-150;let g=ctx.createLinearGradient(beam-90,0,beam+90,0);g.addColorStop(0,\'rgba(255,157,10,0)\');g.addColorStop(.5,\'rgba(255,157,10,.035)\');g.addColorStop(1,\'rgba(255,157,10,0)\');ctx.fillStyle=g;ctx.fillRect(beam-90,0,180,h);\n raf=requestAnimationFrame(draw)}\nconst reduce=matchMedia(\'(prefers-reduced-motion: reduce)\').matches;resize();if(!reduce)raf=requestAnimationFrame(draw);else draw(0);addEventListener(\'resize\',resize);})();\n</script>\n</body>\n</html>'

LOGIN_PAGE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Login - Stock Finder Pro</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #0a0a0c; --card: rgba(24, 24, 28, 0.85); --border: rgba(255, 255, 255, 0.08);
      --text: #e0e0e0; --muted: #8b8b93; --primary: #f39c12; --strong-text: #ffffff; --red: #e74c3c;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Plus Jakarta Sans', sans-serif; }
    body { background-color: var(--bg); color: var(--text); height: 100vh; display: flex; justify-content: center; align-items: center; }
    .login-card { background: var(--card); border: 1px solid var(--border); border-radius: 16px; padding: 2.5rem; width: 100%; max-width: 400px; box-shadow: 0 20px 50px rgba(0,0,0,0.8); backdrop-filter: blur(16px); }
    .brand { font-weight: 800; font-size: 1.25rem; color: var(--primary); margin-bottom: 1.5rem; text-align: center; }
    .form-group { margin-bottom: 1.2rem; }
    label { display: block; font-size: 0.75rem; font-weight: 700; color: var(--muted); margin-bottom: 0.4rem; text-transform: uppercase; }
    input { width: 100%; background: rgba(0, 0, 0, 0.4); border: 1px solid var(--border); padding: 0.8rem 1rem; border-radius: 10px; color: var(--strong-text); font-size: 0.9rem; outline: none; }
    input:focus { border-color: var(--primary); box-shadow: 0 0 10px rgba(243, 156, 18, 0.2); }
    .btn-login { width: 100%; background: var(--primary); color: #000; padding: 0.9rem; border-radius: 10px; font-weight: 700; border: none; cursor: pointer; font-size: 0.95rem; margin-top: 1rem; transition: background 0.2s; }
    .btn-login:hover { background: #e67e22; }
    .forgot-link { display: block; text-align: right; margin-top: 0.5rem; font-size: 0.75rem; color: var(--primary); text-decoration: none; }
    .forgot-link:hover { text-decoration: underline; }
    .back-link { display: block; text-align: center; margin-top: 1rem; font-size: 0.78rem; color: var(--muted); text-decoration: none; }
    .back-link:hover { color: var(--strong-text); }
    .error-msg { background: rgba(231, 76, 60, 0.15); border: 1px solid var(--red); color: var(--red); padding: 0.6rem; border-radius: 8px; font-size: 0.75rem; margin-bottom: 1rem; text-align: center; }
  </style>
</head>
<body>
  <div class="login-card">
    <div class="brand">⚡ TERMINAL LOGIN</div>
    {% if error %}
      <div class="error-msg">{{ error }}</div>
    {% endif %}
    <form action="/login" method="POST">
      <div class="form-group">
        <label>Email Address</label>
        <input type="email" name="email" placeholder="trader@example.com" required />
      </div>
      <div class="form-group">
        <label>Password</label>
        <input type="password" name="password" placeholder="••••••••" required />
        <a href="/forgot-password" class="forgot-link">Forgot Password?</a>
      </div>
      <button type="submit" class="btn-login">Login to Terminal 🚀</button>
    </form>
    <a href="/signup" class="back-link">Don't have an account? Sign Up</a>
  </div>
</body>
</html>
"""

SIGNUP_PAGE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Sign Up - Stock Finder Pro</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #0a0a0c; --card: rgba(24, 24, 28, 0.85); --border: rgba(255, 255, 255, 0.08);
      --text: #e0e0e0; --muted: #8b8b93; --primary: #f39c12; --strong-text: #ffffff; --red: #e74c3c;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Plus Jakarta Sans', sans-serif; }
    body { background-color: var(--bg); color: var(--text); height: 100vh; display: flex; justify-content: center; align-items: center; }
    .signup-card { background: var(--card); border: 1px solid var(--border); border-radius: 16px; padding: 2.5rem; width: 100%; max-width: 400px; box-shadow: 0 20px 50px rgba(0,0,0,0.8); backdrop-filter: blur(16px); }
    .brand { font-weight: 800; font-size: 1.25rem; color: var(--primary); margin-bottom: 1.5rem; text-align: center; }
    .form-group { margin-bottom: 1.2rem; }
    label { display: block; font-size: 0.75rem; font-weight: 700; color: var(--muted); margin-bottom: 0.4rem; text-transform: uppercase; }
    input { width: 100%; background: rgba(0, 0, 0, 0.4); border: 1px solid var(--border); padding: 0.8rem 1rem; border-radius: 10px; color: var(--strong-text); font-size: 0.9rem; outline: none; }
    input:focus { border-color: var(--primary); box-shadow: 0 0 10px rgba(243, 156, 18, 0.2); }
    .btn-signup { width: 100%; background: var(--primary); color: #000; padding: 0.9rem; border-radius: 10px; font-weight: 700; border: none; cursor: pointer; font-size: 0.95rem; margin-top: 1rem; transition: background 0.2s; }
    .btn-signup:hover { background: #e67e22; }
    .back-link { display: block; text-align: center; margin-top: 1rem; font-size: 0.78rem; color: var(--muted); text-decoration: none; }
    .back-link:hover { color: var(--strong-text); }
    .error-msg { background: rgba(231, 76, 60, 0.15); border: 1px solid var(--red); color: var(--red); padding: 0.6rem; border-radius: 8px; font-size: 0.75rem; margin-bottom: 1rem; text-align: center; }
  </style>
</head>
<body>
  <div class="signup-card">
    <div class="brand">⚡ CREATE ACCOUNT</div>
    {% if error %}
      <div class="error-msg">{{ error }}</div>
    {% endif %}
    <form action="/signup" method="POST"><input type="hidden" name="plan" value="{{ plan|default('monthly') }}">
      <div class="form-group">
        <label>Email Address</label>
        <input type="email" name="email" placeholder="trader@example.com" required />
      </div>
      <div class="form-group">
        <label>Password</label>
        <input type="password" name="password" placeholder="••••••••" required />
      </div>
      <button type="submit" class="btn-signup">Sign Up & Save 🚀</button>
    </form>
    <a href="/login" class="back-link">Already have an account? Login</a>
  </div>
</body>
</html>
"""

FORGOT_PASSWORD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Forgot Password - Stock Finder Pro</title>
  <style>
    :root { --bg: #0a0a0c; --card: rgba(24, 24, 28, 0.85); --border: rgba(255, 255, 255, 0.08); --text: #e0e0e0; --muted: #8b8b93; --primary: #f39c12; --strong-text: #ffffff; --red: #e74c3c; }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Plus Jakarta Sans', sans-serif; }
    body { background-color: var(--bg); color: var(--text); height: 100vh; display: flex; justify-content: center; align-items: center; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 16px; padding: 2.5rem; width: 100%; max-width: 400px; box-shadow: 0 20px 50px rgba(0,0,0,0.8); }
    .brand { font-weight: 800; font-size: 1.25rem; color: var(--primary); margin-bottom: 1.5rem; text-align: center; }
    .form-group { margin-bottom: 1.2rem; }
    label { display: block; font-size: 0.75rem; font-weight: 700; color: var(--muted); margin-bottom: 0.4rem; text-transform: uppercase; }
    input { width: 100%; background: rgba(0, 0, 0, 0.4); border: 1px solid var(--border); padding: 0.8rem 1rem; border-radius: 10px; color: var(--strong-text); font-size: 0.9rem; outline: none; }
    input:focus { border-color: var(--primary); }
    .btn { width: 100%; background: var(--primary); color: #000; padding: 0.9rem; border-radius: 10px; font-weight: 700; border: none; cursor: pointer; font-size: 0.95rem; margin-top: 1rem; }
    .error-msg { background: rgba(231, 76, 60, 0.15); border: 1px solid var(--red); color: var(--red); padding: 0.6rem; border-radius: 8px; font-size: 0.75rem; margin-bottom: 1rem; text-align: center; }
    .back-link { display: block; text-align: center; margin-top: 1rem; font-size: 0.78rem; color: var(--muted); text-decoration: none; }
  </style>
</head>
<body>
  <div class="card">
    <div class="brand">🔒 RESET PASSWORD</div>
    {% if error %}<div class="error-msg">{{ error }}</div>{% endif %}
    <form action="/forgot-password" method="POST">
      <div class="form-group">
        <label>Registered Email</label>
        <input type="email" name="email" placeholder="trader@example.com" required />
      </div>
      <button type="submit" class="btn">Send OTP Code 🚀</button>
    </form>
    <a href="/login" class="back-link">Back to Login</a>
  </div>
</body>
</html>
"""

VERIFY_OTP_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Verify OTP - Stock Finder Pro</title>
  <style>
    :root { --bg: #0a0a0c; --card: rgba(24, 24, 28, 0.85); --border: rgba(255, 255, 255, 0.08); --text: #e0e0e0; --muted: #8b8b93; --primary: #f39c12; --strong-text: #ffffff; --red: #e74c3c; }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Plus Jakarta Sans', sans-serif; }
    body { background-color: var(--bg); color: var(--text); height: 100vh; display: flex; justify-content: center; align-items: center; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 16px; padding: 2.5rem; width: 100%; max-width: 400px; box-shadow: 0 20px 50px rgba(0,0,0,0.8); }
    .brand { font-weight: 800; font-size: 1.25rem; color: var(--primary); margin-bottom: 1.5rem; text-align: center; }
    .form-group { margin-bottom: 1.2rem; }
    label { display: block; font-size: 0.75rem; font-weight: 700; color: var(--muted); margin-bottom: 0.4rem; text-transform: uppercase; }
    input { width: 100%; background: rgba(0, 0, 0, 0.4); border: 1px solid var(--border); padding: 0.8rem 1rem; border-radius: 10px; color: var(--strong-text); font-size: 1.1rem; text-align: center; letter-spacing: 4px; outline: none; }
    input:focus { border-color: var(--primary); }
    .btn { width: 100%; background: var(--primary); color: #000; padding: 0.9rem; border-radius: 10px; font-weight: 700; border: none; cursor: pointer; font-size: 0.95rem; margin-top: 1rem; }
    .error-msg { background: rgba(231, 76, 60, 0.15); border: 1px solid var(--red); color: var(--red); padding: 0.6rem; border-radius: 8px; font-size: 0.75rem; margin-bottom: 1rem; text-align: center; }
  </style>
</head>
<body>
  <div class="card">
    <div class="brand">📬 ENTER 6-DIGIT OTP</div>
    {% if error %}<div class="error-msg">{{ error }}</div>{% endif %}
    <form action="/verify-otp" method="POST">
      <div class="form-group">
        <label>Verification Code</label>
        <input type="text" name="otp" maxlength="6" placeholder="123456" required />
      </div>
      <button type="submit" class="btn">Verify OTP ⚡</button>
    </form>
  </div>
</body>
</html>
"""

RESET_PASSWORD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>New Password - Stock Finder Pro</title>
  <style>
    :root { --bg: #0a0a0c; --card: rgba(24, 24, 28, 0.85); --border: rgba(255, 255, 255, 0.08); --text: #e0e0e0; --muted: #8b8b93; --primary: #f39c12; --strong-text: #ffffff; --red: #e74c3c; }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Plus Jakarta Sans', sans-serif; }
    body { background-color: var(--bg); color: var(--text); height: 100vh; display: flex; justify-content: center; align-items: center; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 16px; padding: 2.5rem; width: 100%; max-width: 400px; box-shadow: 0 20px 50px rgba(0,0,0,0.8); }
    .brand { font-weight: 800; font-size: 1.25rem; color: var(--primary); margin-bottom: 1.5rem; text-align: center; }
    .form-group { margin-bottom: 1.2rem; }
    label { display: block; font-size: 0.75rem; font-weight: 700; color: var(--muted); margin-bottom: 0.4rem; text-transform: uppercase; }
    input { width: 100%; background: rgba(0, 0, 0, 0.4); border: 1px solid var(--border); padding: 0.8rem 1rem; border-radius: 10px; color: var(--strong-text); font-size: 0.9rem; outline: none; }
    input:focus { border-color: var(--primary); }
    .btn { width: 100%; background: var(--primary); color: #000; padding: 0.9rem; border-radius: 10px; font-weight: 700; border: none; cursor: pointer; font-size: 0.95rem; margin-top: 1rem; }
  </style>
</head>
<body>
  <div class="card">
    <div class="brand">🔑 NEW PASSWORD</div>
    <form action="/reset-password" method="POST">
      <div class="form-group">
        <label>Enter New Password</label>
        <input type="password" name="password" placeholder="••••••••" required />
      </div>
      <button type="submit" class="btn">Update Password 🚀</button>
    </form>
  </div>
</body>
</html>
"""

# ================= FLASK ROUTING & AUTHENTICATION =================
@app.route("/")
def landing_page():
    return render_template_string(LANDING_PAGE_HTML)

@app.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password")
        
        print(f"📝 Signup Attempt - Email: {email}, Password Received: {bool(password)}") # డిబగ్ లాగ్
        
        if not email or not password:
            return render_template_string(SIGNUP_PAGE_HTML, error="Email and password are required!")
        
        user_exists = User.query.filter_by(email=email).first()
        if user_exists:
            return render_template_string(SIGNUP_PAGE_HTML, error="Email already registered! Please login.")
        
        try:
            hashed_password = generate_password_hash(password, method='pbkdf2:sha256')
            new_user = User(email=email, password=hashed_password, watchlist="")
            db.session.add(new_user)
            db.session.commit()
            print(f"✅ User successfully created in DB: {email}")
        except Exception as e:
            db.session.rollback()
            print(f"❌ Signup DB Error: {e}")
            return render_template_string(SIGNUP_PAGE_HTML, error="Database error during signup. Try again.")
        
        session.clear()
        session.permanent = True
        session['user'] = email
        return redirect(url_for('checkout', plan=request.form.get('plan') or session.get('selected_plan', 'monthly')))
    selected = request.args.get('plan', 'monthly')
    if selected in PLAN_DETAILS: session['selected_plan'] = selected
    return render_template_string(SIGNUP_PAGE_HTML, plan=selected)

@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password")
        
        print(f"🔐 Login Attempt - Email: {email}") # డిబగ్ లాగ్
        
        if not email or not password:
            return render_template_string(LOGIN_PAGE_HTML, error="Email and password are required!")
            
        user = User.query.filter_by(email=email).first()
        if user:
            password_match = check_password_hash(user.password, password)
            print(f"🔍 User found in DB. Password Match: {password_match}")
            if password_match:
                chosen_plan = session.get('selected_plan', 'monthly')
                session.clear()
                session.permanent = True
                session['user'] = email
                return redirect(url_for('terminal_app') if active_subscription(user) else url_for('checkout', plan=chosen_plan))
            
        return render_template_string(LOGIN_PAGE_HTML, error="Invalid email or password!")
    return render_template_string(LOGIN_PAGE_HTML)

@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        user = User.query.filter_by(email=email).first()
        
        if user:
            otp = str(random.randint(100000, 999999))
            user.reset_otp = otp
            user.otp_expiry = datetime.now(timezone.utc) + timedelta(minutes=10)
            db.session.commit()
            
            email_sent = send_otp_email(email, otp)
            if not email_sent:
                return render_template_string(FORGOT_PASSWORD_HTML, error="Failed to send email. Please check your Resend API key.")
            
            session['reset_email'] = email
            return redirect(url_for('verify_otp'))
        else:
            return render_template_string(FORGOT_PASSWORD_HTML, error="Email address not found!")
            
    return render_template_string(FORGOT_PASSWORD_HTML)

@app.route("/verify-otp", methods=["GET", "POST"])
def verify_otp():
    if 'reset_email' not in session:
        return redirect(url_for('forgot_password'))
        
    if request.method == "POST":
        entered_otp = request.form.get("otp", "").strip()
        email = session['reset_email']
        user = User.query.filter_by(email=email).first()
        
        expiry = user.otp_expiry
        if expiry and expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
            
        if user and user.reset_otp == entered_otp and expiry and expiry > datetime.now(timezone.utc):
            session['otp_verified'] = True
            return redirect(url_for('reset_password'))
        else:
            return render_template_string(VERIFY_OTP_HTML, error="Invalid or expired OTP code.")
            
    return render_template_string(VERIFY_OTP_HTML)

@app.route("/reset-password", methods=["GET", "POST"])
def reset_password():
    if not session.get('otp_verified') or 'reset_email' not in session:
        return redirect(url_for('forgot_password'))
        
    if request.method == "POST":
        new_password = request.form.get("password")
        email = session['reset_email']
        user = User.query.filter_by(email=email).first()
        
        if user:
            user.password = generate_password_hash(new_password, method='pbkdf2:sha256')
            user.reset_otp = None
            user.otp_expiry = None
            db.session.commit()
            
            session.pop('reset_email', None)
            session.pop('otp_verified', None)
            return redirect(url_for('login_page'))
            
    return render_template_string(RESET_PASSWORD_HTML)

@app.route("/terminal")
def terminal_app():
    if 'user' not in session:
        return redirect(url_for('login_page'))
    if not active_subscription(current_user()):
        return redirect(url_for('landing_page') + '#pricing')
    return send_from_directory('.', 'index.html')

@app.route("/logout")
def logout():
    session.pop('user', None)
    return redirect(url_for('landing_page'))

@app.before_request
def protect_market_apis():
    if request.path.startswith('/api/') and not request.path.startswith('/api/payment/'):
        if not active_subscription(current_user()):
            return jsonify({'status':'error','message':'Active subscription required'}), 403

@app.route('/checkout')
def checkout():
    plan = request.args.get('plan', 'monthly')
    if plan not in PLAN_DETAILS: abort(400)
    if not current_user():
        session['selected_plan'] = plan
        return redirect(url_for('signup', plan=plan))
    if not RAZORPAY_KEY_ID or not RAZORPAY_KEY_SECRET:
        return 'Razorpay is not configured. Set RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET.', 503
    amount, _ = PLAN_DETAILS[plan]
    try:
        r = requests.post('https://api.razorpay.com/v1/orders',
            auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET),
            json={'amount':amount,'currency':'INR','receipt':f'u{current_user().id}_{int(time.time())}',
                  'notes':{'plan':plan,'user_id':str(current_user().id)}}, timeout=15)
        r.raise_for_status()
        order = r.json()
        payment = Payment(user_id=current_user().id, order_id=order['id'],plan=plan,amount_paise=amount)
        db.session.add(payment); db.session.commit()
    except Exception:
        app.logger.exception('Razorpay order creation failed')
        return 'Unable to create payment order. Please try again.', 502
    return render_template_string(CHECKOUT_HTML, key_id=RAZORPAY_KEY_ID,
        order_id=order['id'], amount=amount, plan=plan, email=current_user().email)

CHECKOUT_HTML = """<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Secure Checkout - Stock Finder Pro</title><script src="https://checkout.razorpay.com/v1/checkout.js"></script>
<style>body{background:#07090d;color:#eee;font-family:Arial;text-align:center;padding:70px 16px}button{background:#ff9e0b;border:0;border-radius:9px;padding:14px 24px;font-weight:bold;cursor:pointer}a{color:#ff9e0b}</style></head>
<body><h2>Stock Finder Pro</h2><p>Selected plan: {{plan|title}} — ₹{{'%.2f'|format(amount/100)}}</p>
<button id="pay">Open Razorpay Payment</button><p id="status"></p><p><a href="/#pricing">Change plan</a></p>
<script>
const opts={key:{{key_id|tojson}},amount:{{amount|tojson}},currency:'INR',name:'Stock Finder Pro',
 description:{{(plan+' plan')|tojson}},order_id:{{order_id|tojson}},prefill:{email:{{email|tojson}}},
 handler:async function(response){document.getElementById('status').textContent='Verifying payment...';
 try{const r=await fetch('/api/payment/verify',{method:'POST',headers:{'Content-Type':'application/json'},
 body:JSON.stringify(response)});const data=await r.json();if(!r.ok)throw Error(data.message||'Verification failed');
 location.href='/terminal';}catch(e){document.getElementById('status').textContent=e.message+' Please contact support if charged.';}},
 theme:{color:'#ff9e0b'}};
document.getElementById('pay').onclick=()=>new Razorpay(opts).open();
window.addEventListener('load',()=>{if(typeof Razorpay==='function')new Razorpay(opts).open();});
</script></body></html>"""

@app.route('/api/payment/verify', methods=['POST'])
def verify_payment():
    user=current_user()
    if not user: return jsonify({'message':'Login required'}),401
    data=request.get_json(silent=True) or {}
    order_id=data.get('razorpay_order_id','')
    payment_id=data.get('razorpay_payment_id','')
    signature=data.get('razorpay_signature','')
    payment=Payment.query.filter_by(order_id=order_id,user_id=user.id).first()
    if not payment or not all((order_id,payment_id,signature)):
        return jsonify({'message':'Unknown payment order'}),400
    digest=hmac.new(RAZORPAY_KEY_SECRET.encode(),f'{order_id}|{payment_id}'.encode(),hashlib.sha256).hexdigest()
    if not hmac.compare_digest(digest,signature):
        return jsonify({'message':'Invalid payment signature'}),400
    if payment.status == 'paid':
        return jsonify({'status':'success'}) if active_subscription(user) else (jsonify({'message':'Subscription unavailable'}),409)
    try:
        r=requests.get('https://api.razorpay.com/v1/payments/'+payment_id,
            auth=(RAZORPAY_KEY_ID,RAZORPAY_KEY_SECRET),timeout=15)
        r.raise_for_status(); gateway_payment=r.json()
    except Exception:
        app.logger.exception('Razorpay payment lookup failed')
        return jsonify({'message':'Could not confirm payment. Please retry.'}),502
    if (gateway_payment.get('status') != 'captured' or gateway_payment.get('order_id') != order_id
        or gateway_payment.get('amount') != payment.amount_paise or gateway_payment.get('currency') != 'INR'):
        return jsonify({'message':'Payment not captured or amount mismatch'}),409
    existing=Payment.query.filter_by(payment_id=payment_id).first()
    if existing and existing.id != payment.id:
        return jsonify({'message':'Payment already used'}),409
    sub=Subscription.query.filter_by(user_id=user.id).first()
    base=max(datetime.utcnow(),sub.expires_at) if sub else datetime.utcnow()
    expires=add_months(base,PLAN_DETAILS[payment.plan][1])
    if sub: sub.plan=payment.plan;sub.expires_at=expires
    else: db.session.add(Subscription(user_id=user.id,plan=payment.plan,expires_at=expires))
    payment.payment_id=payment_id;payment.status='paid'
    db.session.commit()
    return jsonify({'status':'success','redirect':'/terminal'})

@app.route("/api/watchlist", methods=["GET"])
def get_watchlist():
    if 'user' not in session:
        return jsonify({"status": "error", "message": "Unauthorized"}), 401
    user = User.query.filter_by(email=session['user']).first()
    if not user or not user.watchlist:
        return jsonify({"status": "success", "data": []})
    symbols = [s.strip().upper() for s in user.watchlist.split(",") if s.strip()]
    return jsonify({"status": "success", "data": symbols})

@app.route("/api/watchlist/toggle", methods=["POST"])
def toggle_watchlist():
    if 'user' not in session:
        return jsonify({"status": "error", "message": "Unauthorized"}), 401
    data = request.json
    symbol = data.get('symbol')
    if not symbol:
        return jsonify({"status": "error", "message": "Symbol required"}), 400
    
    symbol = symbol.strip().upper()
    user = User.query.filter_by(email=session['user']).first()
    if not user:
        return jsonify({"status": "error", "message": "User not found"}), 404

    current_symbols = [s.strip().upper() for s in user.watchlist.split(",") if s.strip()]
    if symbol in current_symbols:
        current_symbols.remove(symbol)
        action = "removed"
    else:
        current_symbols.append(symbol)
        action = "added"

    user.watchlist = ",".join(current_symbols)
    db.session.commit()
    return jsonify({"status": "success", "action": action, "symbol": symbol, "data": current_symbols})

@app.route("/api/smart-orb", methods=["GET"])
def get_smart_orb():
    try:
        if SMART_ORB_SIGNALS:
            return jsonify({"status": "success", "timestamp": datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%H:%M:%S"), "data": SMART_ORB_SIGNALS})
        stocks = fetch_live_upstox_quotes("FO")
        if isinstance(stocks, dict) and "error" in stocks or not isinstance(stocks, list):
            return jsonify({"status": "success", "data": []})
        advances = sum(1 for s in stocks if s.get('pricePct', 0) > 0)
        declines = sum(1 for s in stocks if s.get('pricePct', 0) < 0)
        market_is_bullish = advances >= declines
        scored_stocks = []
        for stock in stocks:
            score, bias = 0, "NEUTRAL"
            price_pct, buildup, candle_tag, vol_multiplier = stock.get('pricePct', 0), stock.get('buildup', ''), stock.get('candleTag', ''), stock.get('volMultiplier', 1.0)
            if market_is_bullish:
                if candle_tag == 'OPEN=LOW': score += 40
                if buildup in ['Long Buildup', 'Short Covering']: score += 30
                if vol_multiplier >= 1.1: score += 20
                if price_pct > 0: score += 10
                if score >= 10: bias = "BULLISH_ORB"
            else:
                if candle_tag == 'OPEN=HIGH': score += 40
                if buildup in ['Short Buildup', 'Long Unwinding']: score += 30
                if vol_multiplier >= 1.1: score += 20
                if price_pct < 0: score += 10
                if score >= 10: bias = "BEARISH_ORB"
            if bias != "NEUTRAL":
                scored_stocks.append({
                    "symbol": stock["symbol"], "bias": bias, "score": score,
                    "ltp": stock["ltp"], "sector": stock["sector"],
                    "reason": f"Score: {score} | Tag: {candle_tag or 'N/A'} | Buildup: {buildup or 'N/A'} | Vol: {vol_multiplier}x"
                })
        scored_stocks.sort(key=lambda x: x['score'], reverse=True)
        return jsonify({"status": "success", "timestamp": datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%H:%M:%S"), "data": scored_stocks[:2]})
    except Exception:
        return jsonify({"status": "success", "data": []})

@app.route("/api/zone-screener", methods=["GET"])
def zone_screener():
    start_total_time = time.time()
    try:
        global ZONE_CACHE
        tf = request.args.get("tf", "day").lower()
        force_refresh = request.args.get("t") is not None
        univ_key, target_universe = get_active_universe_list()
        
        # quarterly మరియు halfyear లను ఇక్కడ చేర్చాము
        valid_tfs = ["5minute", "15minute", "30minute", "60minute", "day", "week", "month", "quarter", "halfyear", "year"]
        if tf not in valid_tfs: tf = "day"
            
        with ZONE_CACHE_LOCK:
            univ_zone_cache = ZONE_CACHE.setdefault(univ_key, {"last_updated": {}})
            if not force_refresh and tf in univ_zone_cache and (time.time() - univ_zone_cache["last_updated"].get(tf, 0) < 600):
                print(f"⚡ Zone cache hit: {univ_key}/{tf}")
                return jsonify({"status": "success", "data": univ_zone_cache[tf]})

        quotes_res = fetch_live_upstox_quotes(univ_key)
        live_quotes = {s["symbol"]: s["ltp"] for s in (quotes_res if isinstance(quotes_res, list) else [])}
        results = []

        active_sample = target_universe 

        def worker(item):
            try:
                candles = fetch_historical_candles(item["isin"], tf)
                if not candles or len(candles) < 16: return []
                ltp = live_quotes.get(item["symbol"], candles[-1][4] if candles else 0)
                zones = scan_four_institutional_zones(candles, ltp, item["symbol"], tf)
                return [{
                    "symbol": item["symbol"], "sector": item["sector"], "ltp": ltp,
                    "pattern": z["pattern"], "bias": z["bias"], "proximal": z["proximal"], "distal": z["distal"],
                    "entry": z["entry"], "stopLoss": z["stopLoss"], "risk": z["risk"], "riskPct": z["riskPct"],
                    "target1R2": z["target1R2"], "target1R3": z["target1R3"],
                    "status": z["status"], "multiplier": z.get("multiplier", 1.0),
                    "tvUrl": z["tvUrl"], "brokerUrl": z["brokerUrl"], "timeframe": z["timeframe"]
                } for z in zones]
            except Exception:
                return []

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            for res in executor.map(worker, active_sample):
                if res: results.extend(res)

        results.sort(key=lambda x: (0 if x["bias"] == "DEMAND" else 1, -x.get("multiplier", 1.0)))
        
        with ZONE_CACHE_LOCK:
            univ_zone_cache[tf] = results
            univ_zone_cache["last_updated"][tf] = time.time()
        print(f"✅ Zone scan {univ_key}/{tf}: {len(results)} zones in {time.time() - start_total_time:.2f}s")
        return jsonify({"status": "success", "data": results})
    except Exception as e:
        print(f"❌ Zone Screener Error: {e}")
        return jsonify({"status": "success", "data": []})

@app.route("/api/screener", methods=["GET"])
def screener():
    filter_val, search_val, univ_type = request.args.get("filter", "ALL"), request.args.get("search", "").strip().lower(), request.args.get("universe", "FO").upper()
    stocks = fetch_live_upstox_quotes(univ_type)
    if isinstance(stocks, dict) and "error" in stocks: return jsonify({"status": "error", "message": stocks["error"]})
    top_gainers = [s for s in sorted(stocks, key=lambda x: float(x.get("pricePct", 0)), reverse=True) if float(s.get("pricePct", 0)) > 0][:20]
    top_losers = [s for s in sorted(stocks, key=lambda x: float(x.get("pricePct", 0))) if float(s.get("pricePct", 0)) < 0][:20]
    filtered = list(stocks)
    if filter_val and filter_val != "ALL":
        if filter_val in ["OPEN=LOW", "OPEN=HIGH"]: filtered = [x for x in filtered if x.get("candleTag") == filter_val]
        elif filter_val == "VOL_SHOCKER": filtered = [x for x in filtered if x.get("volMultiplier", 0) >= 1.5]
        else: filtered = [x for x in filtered if x.get("buildup", "").lower() == filter_val.lower()]
    if search_val: filtered = [x for x in filtered if search_val in x["symbol"].lower() or search_val in x["name"].lower()]
    return jsonify({"status": "success", "universe": univ_type, "timestamp": datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%H:%M:%S"), "count": len(filtered), "data": filtered, "topGainers": top_gainers, "topLosers": top_losers})

@app.route("/api/sectors", methods=["GET"])
def get_sectors():
    univ_type = request.args.get("universe", "FO").upper()
    stocks = fetch_live_upstox_quotes(univ_type)
    if isinstance(stocks, dict) and "error" in stocks: return jsonify({"status": "error", "message": stocks["error"]})
    target_stocks = [s for s in stocks if s.get("is_fo", True)] if univ_type == "FO" else stocks
    sec_map = defaultdict(lambda: {"stocks": 0, "total_pct": 0.0, "total_vol": 0, "advances": 0, "declines": 0})
    total_vol = total_adv = total_dec = 0
    for s in target_stocks:
        sec, p_pct, vol = s.get("sector", "Market"), float(s.get("pricePct", 0.0)), int(s.get("volume", 0))
        total_vol += vol
        if p_pct >= 0: total_adv += 1
        else: total_dec += 1
        sec_map[sec]["stocks"] += 1
        sec_map[sec]["total_pct"] += p_pct
        sec_map[sec]["total_vol"] += vol
        if p_pct >= 0: sec_map[sec]["advances"] += 1
        else: sec_map[sec]["declines"] += 1
    summary = [{"sector": sec, "avgChange": round(val["total_pct"] / val["stocks"], 2) if val["stocks"] > 0 else 0.0, "stocksCount": val["stocks"], "advances": val["advances"], "declines": val["declines"], "totalVolume": val["total_vol"]} for sec, val in sec_map.items()]
    summary.sort(key=lambda x: x["avgChange"], reverse=True)
    return jsonify({"status": "success", "universe": univ_type, "data": summary, "marketBreadth": {"totalVolume": total_vol, "advances": total_adv, "declines": total_dec}})

@app.route("/api/ticker-bar", methods=["GET"])
def ticker_bar():
    univ_type = request.args.get("universe", "FO").upper()
    headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}
    ticker_results = []
    index_isins = [idx["isin"] for idx in MARKET_INDICES]
    url = "https://api.upstox.com/v2/market-quote/quotes"
    
    try:
        res = HTTP_SESSION.get(url, headers=headers, params={"instrument_key": ",".join(index_isins)}, timeout=4)
        if res.status_code == 200:
            data = res.json().get("data", {})
            for idx in MARKET_INDICES:
                q = data.get(idx["isin"]) or data.get(idx["isin"].replace("|", ":"))
                if q:
                    ltp = float(q.get("last_price", 0.0))
                    net_change = float(q.get("net_change", 0.0))
                    prev_close = float(q.get("prev_close") or (ltp - net_change))
                    price_pct = round(((ltp - prev_close) / prev_close) * 100, 2) if prev_close > 0 else 0.0
                    ticker_results.append({
                        "symbol": idx["symbol"], "price": f"{ltp:,.2f}",
                        "change": f"{net_change:+,.2f} ({price_pct:+.2f}%)", "isPositive": net_change >= 0
                    })
    except Exception:
        pass

    stocks = fetch_live_upstox_quotes(univ_type)
    if isinstance(stocks, list) and len(stocks) > 0:
        sec_map = defaultdict(lambda: {"total_pct": 0.0, "stocks": 0})
        for s in stocks:
            sec = s.get("sector", "")
            if sec:
                sec_map[sec]["total_pct"] += float(s.get("pricePct", 0.0))
                sec_map[sec]["stocks"] += 1
        
        for sec, val in sec_map.items():
            if val["stocks"] > 0:
                avg_pct = round(val["total_pct"] / val["stocks"], 2)
                ticker_results.append({
                    "symbol": sec,
                    "price": "",
                    "change": f"{avg_pct:+.2f}%",
                    "isPositive": avg_pct >= 0
                })

        sorted_by_gain = sorted(stocks, key=lambda x: float(x.get("pricePct", 0.0)), reverse=True)
        if len(sorted_by_gain) > 0:
            top_gainer = sorted_by_gain[0]
            ticker_results.append({"symbol": f"TOP GAINER: {top_gainer['symbol']}", "price": f"{top_gainer['ltp']:,.2f}", "change": f"{top_gainer['pricePct']:+.2f}%", "isPositive": True})
            top_loser = sorted_by_gain[-1]
            ticker_results.append({"symbol": f"TOP LOSER: {top_loser['symbol']}", "price": f"{top_loser['ltp']:,.2f}", "change": f"{top_loser['pricePct']:+.2f}%", "isPositive": False})

    return jsonify({"status": "success", "data": ticker_results})

if __name__ == "__main__":
    start_smart_orb_background_thread()
    port = int(os.environ.get('PORT', 5000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)

