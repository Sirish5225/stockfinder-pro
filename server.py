import os
import json
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
app.secret_key = os.getenv("FLASK_SECRET_KEY", "9f31b85adc90a103684b3c46ac83610a28f9a7b0a99d92bf2c0872e84e8591fe")
if not app.secret_key:
    raise RuntimeError("Set FLASK_SECRET_KEY in your environment before starting StockFinder Pro")
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=31)

# Absolute Path Fix for SQLite Database to prevent 'no such table' errors
basedir = os.path.abspath(os.path.dirname(__file__))
db_path = os.getenv('DATABASE_PATH', os.path.join(basedir, 'users.db'))
database_url = os.getenv('DATABASE_URL', '').strip()
if database_url.startswith('postgres://'):
    database_url = 'postgresql://' + database_url[len('postgres://'):]
app.config['SQLALCHEMY_DATABASE_URI'] = database_url or ('sqlite:///' + db_path)
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

class HistoricalCandleStore(db.Model):
    __tablename__ = 'historical_candle_store'
    cache_key = db.Column(db.String(180), primary_key=True)
    candles_json = db.Column(db.Text, nullable=False)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class ZoneResultStore(db.Model):
    __tablename__ = 'zone_result_store'
    cache_key = db.Column(db.String(100), primary_key=True)
    zones_json = db.Column(db.Text, nullable=False)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class Subscription(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), unique=True, nullable=False)
    plan = db.Column(db.String(20), nullable=False)
    expires_at = db.Column(db.DateTime, nullable=False)

PLAN_DETAILS = {'monthly': (49900, 1), 'quarterly': (119900, 3),
                'halfyearly': (249900, 6), 'yearly': (499900, 12)}
RAZORPAY_KEY_ID = os.getenv('RAZORPAY_KEY_ID', '').strip()
RAZORPAY_KEY_SECRET = os.getenv('RAZORPAY_KEY_SECRET', '').strip()

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
    NIFTY500_UNIVERSE_CACHE = universe[:500]
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
    NIFTY500_UNIVERSE_CACHE = universe[:500]
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
    TOTAL_MARKET_UNIVERSE_CACHE = universe
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
    """Build completed calendar quarter/half-year/year candles from Upstox monthly OHLC."""
    if not candles:
        return []
    period_months = {'quarter': 3, 'halfyear': 6, 'year': 12}.get(timeframe)
    if not period_months:
        return candles
    buckets = {}
    for c in candles:
        try:
            stamp = pd.to_datetime(c[0], errors='coerce')
            if pd.isna(stamp):
                continue
            month = int(stamp.month)
            start_month = ((month - 1) // period_months) * period_months + 1
            key = (int(stamp.year), start_month)
            buckets.setdefault(key, []).append((stamp, c))
        except (ValueError, TypeError, IndexError):
            continue
    today = datetime.now(pytz.timezone('Asia/Kolkata')).date()
    rows = []
    for (year, start_month), entries in sorted(buckets.items()):
        entries.sort(key=lambda pair: pair[0])
        distinct_months = {int(stamp.month) for stamp, _ in entries}
        if len(distinct_months) != period_months or distinct_months != set(range(start_month, start_month + period_months)):
            continue
        # Do not build a higher-timeframe candle from the still-forming month.
        last_month = start_month + period_months - 1
        if year == today.year and start_month <= today.month <= last_month:
            continue
        cs = [c for _, c in entries]
        rows.append([cs[0][0], float(cs[0][1]), max(float(c[2]) for c in cs),
                     min(float(c[3]) for c in cs), float(cs[-1][4]),
                     sum(float(c[5]) for c in cs)])
    return rows

def has_strong_exit_confirmation(candles, legout_idx, bias):
    """One immediate same-direction exit candle with >=70% body and breakout close."""
    if legout_idx + 1 >= len(candles):
        return False
    legout, candle = candles[legout_idx], candles[legout_idx + 1]
    o, h, l, c = map(float, (candle[1], candle[2], candle[3], candle[4]))
    if h <= l or abs(c - o) / (h - l) < 0.70:
        return False
    return (c > o and c > float(legout[2])) if bias == 'DEMAND' else (c < o and c < float(legout[3]))


def scan_four_institutional_zones(candles, current_ltp, symbol, tf, max_dist_pct=None):
    """Strict RBR/DBR/RBD/DBD: exactly 1-2 contiguous compact base candles."""
    zones = []
    if not candles or len(candles) < 2 or current_ltp <= 0:
        return zones
    # A forming monthly candle is not a confirmed leg-in, base, or leg-out.
    # Derived quarterly/half-yearly candles are already restricted to completed periods.
    if tf == 'month':
        now_ist = datetime.now(pytz.timezone('Asia/Kolkata'))
        completed = []
        for candle in candles:
            stamp = pd.to_datetime(candle[0], errors='coerce')
            if pd.isna(stamp):
                continue
            if (int(stamp.year), int(stamp.month)) < (now_ist.year, now_ist.month):
                completed.append(candle)
        candles = completed
        if len(candles) < 3:
            return zones

    # Upstox may return newest-first; all structure/retest logic needs oldest-first.
    candles = sorted({str(c[0]): c for c in candles if len(c) >= 5}.values(), key=lambda c: str(c[0]))

    def metrics(c):
        o, h, l, close = map(float, (c[1], c[2], c[3], c[4]))
        span = h - l
        if span <= 0 or not (l <= min(o, close) <= max(o, close) <= h):
            return None
        return {'o': o, 'h': h, 'l': l, 'c': close,
                'body': abs(close-o), 'ratio': abs(close-o)/span}

    m = [metrics(c) for c in candles]

    def compact(j, leg_body):
        v = m[j]
        return bool(v and v['ratio'] <= 0.30 and v['body'] <= 0.35*leg_body)

    # Iterate each leg-in once, count the FULL contiguous base sequence.
    for legin_idx in range(len(candles)-2):
        legin = m[legin_idx]
        if not legin or not (0.70 <= legin['ratio'] <= 0.98):
            continue
        first_base = legin_idx+1
        if not compact(first_base, legin['body']):
            continue
        legout_idx = first_base
        while legout_idx < len(candles) and compact(legout_idx, legin['body']):
            legout_idx += 1
        num_bases = legout_idx-first_base
        if num_bases not in (1, 2) or legout_idx >= len(candles):
            continue
        legout = m[legout_idx]
        if not legout or not (0.75 <= legout['ratio'] <= 0.98):
            continue
        if legout['body'] < legin['body']:
            continue
        base = m[first_base:legout_idx]
        base_high = max(c['h'] for c in base)
        base_low = min(c['l'] for c in base)
        # Reject wide, scattered consolidation even if individual candles are dojis.
        if base_high-base_low > max(legin['body'], legout['body']):
            continue

        legin_rally = legin['c'] > legin['o']
        legout_rally = legout['c'] > legout['o']
        if legout_rally:
            bias = 'DEMAND'
            pattern = 'Rally-Base-Rally' if legin_rally else 'Drop-Base-Rally'
            proximal = max(max(c['o'], c['c']) for c in base)
            distal = base_low
            # A leg-out must CLOSE outside the full base range.
            if legout['c'] <= base_high or legout['c'] <= legout['o']:
                continue
        else:
            bias = 'SUPPLY'
            pattern = 'Rally-Base-Drop' if legin_rally else 'Drop-Base-Drop'
            proximal = min(min(c['o'], c['c']) for c in base)
            distal = base_high
            if legout['c'] >= base_low or legout['c'] >= legout['o']:
                continue

        risk = abs(proximal-distal)
        if risk <= 0:
            continue
        distance_pct = round(abs(current_ltp-proximal)/current_ltp*100, 2)
        if max_dist_pct is not None and distance_pct > max_dist_pct:
            continue

        # Count separate retest EVENTS, not every candle that touches the zone.
        test_count, violated, inside_previous = 0, False, False
        for later in m[legout_idx+1:]:
            if later is None:
                continue
            if bias == 'DEMAND':
                touched = later['l'] <= proximal and later['h'] >= distal
                violated = later['c'] < distal
            else:
                touched = later['h'] >= proximal and later['l'] <= distal
                violated = later['c'] > distal
            if violated:
                break
            if touched and not inside_previous:
                test_count += 1
            inside_previous = touched
            if test_count > 1:
                break
        if violated or test_count > 1:
            continue
        if bias == 'DEMAND' and current_ltp < distal:
            continue
        if bias == 'SUPPLY' and current_ltp > distal:
            continue
        in_zone = min(proximal, distal) <= current_ltp <= max(proximal, distal)
        status = '🎯 In Zone (Actionable)' if in_zone else ('Untested (Fresh)' if test_count == 0 else f'Tested ({test_count}x)')
        zones.append({
            'pattern': pattern, 'bias': bias, 'proximal': round(proximal, 2),
            'distal': round(distal, 2), 'entry': round(proximal, 2),
            'stopLoss': round(distal, 2), 'risk': round(risk, 2),
            'riskPct': round(risk/current_ltp*100, 2),
            'target1R2': round(proximal+2*risk if bias == 'DEMAND' else proximal-2*risk, 2),
            'target1R3': round(proximal+3*risk if bias == 'DEMAND' else proximal-3*risk, 2),
            'status': status, 'distancePct': distance_pct,
            'multiplier': round(legout['body']/max(legin['body'], 0.01), 2),
            'strongZone': has_strong_exit_confirmation(candles, legout_idx, bias),
            'tvUrl': f'https://www.tradingview.com/chart/?symbol=NSE:{symbol}',
            'brokerUrl': f'https://groww.in/stocks/{symbol.lower()}',
            'timeframe': tf.upper(), 'baseCandles': num_bases
        })
    # Base-free reversal patterns: DR / RD (two adjacent completed candles).
    # These are separate from RBR/DBR/RBD/DBD; they have ZERO base candles.
    for incoming_idx in range(len(m) - 1):
        outgoing_idx = incoming_idx + 1
        incoming, outgoing = m[incoming_idx], m[outgoing_idx]
        if not incoming or not outgoing:
            continue
        if not (0.70 <= incoming['ratio'] <= 0.98 and
                0.75 <= outgoing['ratio'] <= 0.98 and
                outgoing['body'] > incoming['body']):
            continue
        if incoming['c'] < incoming['o'] and outgoing['c'] > outgoing['o']:
            bias, pattern = 'DEMAND', 'Drop-Rally'
            proximal = min(outgoing['o'], outgoing['c'])
            distal = outgoing['l']  # Second (Rally) candle lower wick
            # Exit must break above the incoming candle, not just turn green.
            if outgoing['c'] <= incoming['o'] or proximal <= distal:
                continue
        elif incoming['c'] > incoming['o'] and outgoing['c'] < outgoing['o']:
            bias, pattern = 'SUPPLY', 'Rally-Drop'
            proximal = max(outgoing['o'], outgoing['c'])
            distal = outgoing['h']  # Second (Drop) candle upper wick
            if outgoing['c'] >= incoming['o'] or proximal >= distal:
                continue
        else:
            continue

        risk = abs(proximal - distal)
        if risk <= 0:
            continue
        distance_pct = round(abs(current_ltp - proximal) / current_ltp * 100, 2)
        if max_dist_pct is not None and distance_pct > max_dist_pct:
            continue
        test_count, violated, inside_previous = 0, False, False
        for later in m[outgoing_idx + 1:]:
            if later is None:
                continue
            if bias == 'DEMAND':
                touched = later['l'] <= proximal and later['h'] >= distal
                violated = later['c'] < distal
            else:
                touched = later['h'] >= proximal and later['l'] <= distal
                violated = later['c'] > distal
            if violated:
                break
            if touched and not inside_previous:
                test_count += 1
            inside_previous = touched
            if test_count > 1:
                break
        if violated or test_count > 1:
            continue
        if bias == 'DEMAND' and current_ltp < distal:
            continue
        if bias == 'SUPPLY' and current_ltp > distal:
            continue
        in_zone = min(proximal, distal) <= current_ltp <= max(proximal, distal)
        status = ('🎯 In Zone (Actionable)' if in_zone else
                  'Untested (Fresh)' if test_count == 0 else f'Tested ({test_count}x)')
        zones.append({
            'pattern': pattern, 'bias': bias, 'proximal': round(proximal, 2),
            'distal': round(distal, 2), 'entry': round(proximal, 2),
            'stopLoss': round(distal, 2), 'risk': round(risk, 2),
            'riskPct': round(risk/current_ltp*100, 2),
            'target1R2': round(proximal+2*risk if bias == 'DEMAND' else proximal-2*risk, 2),
            'target1R3': round(proximal+3*risk if bias == 'DEMAND' else proximal-3*risk, 2),
            'status': status, 'distancePct': distance_pct,
            'multiplier': round(outgoing['body']/max(incoming['body'], 0.01), 2),
            'strongZone': has_strong_exit_confirmation(candles, outgoing_idx, bias),
            'tvUrl': f'https://www.tradingview.com/chart/?symbol=NSE:{symbol}',
            'brokerUrl': f'https://groww.in/stocks/{symbol.lower()}',
            'timeframe': tf.upper(), 'baseCandles': 0,
            # DR/RD have no base candle. Zone boundaries come from candle 2.
            'zoneCandleDate': str(candles[outgoing_idx][0]),
            'zoneCandleOpen': outgoing['o'],
            'zoneCandleHigh': outgoing['h'],
            'zoneCandleLow': outgoing['l'],
            'zoneCandleClose': outgoing['c'],
            'incomingCandleDate': str(candles[incoming_idx][0]),
            'exitCandleDate': str(candles[outgoing_idx][0]),
            'exitCandleOpen': outgoing['o'],
            'exitCandleHigh': outgoing['h'],
            'exitCandleLow': outgoing['l'],
            'exitCandleClose': outgoing['c'],
            'zoneBoundarySource': 'SECOND_CANDLE_ONLY'
        })
    return zones

# Four-year persistent candle store. PostgreSQL survives Render deploys/restarts;
# local SQLite is only durable when stored on a persistent disk.
HISTORY_FETCH_LOCKS = {}
HISTORY_FETCH_LOCKS_GUARD = threading.Lock()
HISTORY_CACHE_SECONDS = int(os.getenv("SFP_HISTORY_CACHE_SECONDS", str(3 * 24 * 3600)))
HISTORY_YEARS = 4

def _read_stored_candles(key):
    try:
        with app.app_context():
            row = db.session.get(HistoricalCandleStore, key)
            if row:
                return json.loads(row.candles_json), row.updated_at
    except Exception as exc:
        print('Historical DB read:', type(exc).__name__, str(exc)[:120])
    return None, None

def _save_stored_candles(key, candles):
    try:
        with app.app_context():
            row = db.session.get(HistoricalCandleStore, key)
            if row is None:
                row = HistoricalCandleStore(cache_key=key, candles_json='[]')
                db.session.add(row)
            row.candles_json = json.dumps(candles, separators=(',', ':'))
            row.updated_at = datetime.utcnow()
            db.session.commit()
    except Exception as exc:
        db.session.rollback()
        print('Historical DB write:', type(exc).__name__, str(exc)[:120])

HISTORY_API_SLOTS = threading.BoundedSemaphore(int(os.getenv('SFP_HISTORY_API_CONCURRENCY', '6')))

def fetch_historical_candles(instrument_key, interval):
    """Shared monthly cache for higher timeframes; incremental refresh after first download."""
    api_interval = 'month' if interval in ('quarter', 'halfyear', 'year') else interval
    raw_key = f'{instrument_key}_{api_interval}_4y_v1'
    derived_key = f'{instrument_key}_{interval}_4y_v1'
    def memory_get(key):
        with CANDLE_CACHE_LOCK:
            entry = CANDLE_CACHE.get(key)
            return entry['candles'] if entry and time.time()-entry['ts'] < HISTORY_CACHE_SECONDS else None
    cached = memory_get(derived_key)
    if cached is not None:
        return cached
    with HISTORY_FETCH_LOCKS_GUARD:
        lock = HISTORY_FETCH_LOCKS.setdefault(raw_key, threading.Lock())
    with lock:
        raw = memory_get(raw_key)
        if raw is None:
            stored, updated_at = _read_stored_candles(raw_key)
            fresh = stored is not None and updated_at is not None and (datetime.utcnow()-updated_at).total_seconds() < HISTORY_CACHE_SECONDS
            if fresh:
                raw = stored
            else:
                today = datetime.now(pytz.timezone('Asia/Kolkata')).date()
                oldest = today - timedelta(days=1461)
                windows = []
                if stored:
                    try:
                        last_date = max(pd.to_datetime(c[0], utc=True).date() for c in stored if len(c)>=6)
                        begin = max(oldest, last_date-timedelta(days=45))
                    except (ValueError, TypeError):
                        begin = max(oldest, today-timedelta(days=65))
                    windows = [(begin,today)]
                else:
                    end_date = today
                    while end_date >= oldest:
                        begin = max(oldest,end_date-timedelta(days=364))
                        windows.append((begin,end_date))
                        end_date = begin-timedelta(days=1)
                headers = {'Accept':'application/json','Authorization':f'Bearer {UPSTOX_ACCESS_TOKEN}'}
                downloaded=[]
                errors=0
                for begin,end_date in windows:
                    url = (f'https://api.upstox.com/v2/historical-candle/'
                           f'{urllib.parse.quote(instrument_key,safe="")}/{api_interval}/'
                           f'{end_date:%Y-%m-%d}/{begin:%Y-%m-%d}')
                    chunk=None
                    for attempt in range(3):
                        try:
                            with HISTORY_API_SLOTS:
                                response=HTTP_SESSION.get(url,headers=headers,timeout=(5,12))
                            if response.status_code==200:
                                chunk=response.json().get('data',{}).get('candles',[])
                                break
                            print(f'Historical {api_interval} HTTP {response.status_code} for {instrument_key}')
                            if response.status_code not in (429,500,502,503,504):
                                break
                        except requests.RequestException as exc:
                            if attempt==2: print('Historical request:',type(exc).__name__)
                        if attempt<2: time.sleep(0.8*(2**attempt)+random.uniform(0,0.4))
                    if chunk is None: errors+=1
                    else: downloaded.extend(chunk)
                if errors==0:
                    by_stamp={str(c[0]):c for c in (stored or []) if len(c)>=6}
                    by_stamp.update({str(c[0]):c for c in downloaded if len(c)>=6})
                    raw=[by_stamp[k] for k in sorted(by_stamp)]
                    if raw: _save_stored_candles(raw_key,raw)
                else:
                    raw=stored if stored is not None else []
                    print(f'Historical refresh incomplete {instrument_key}/{api_interval}: {errors}/{len(windows)} windows failed')
            with CANDLE_CACHE_LOCK:
                CANDLE_CACHE[raw_key]={'candles':raw,'ts':time.time() if raw else time.time()-HISTORY_CACHE_SECONDS+60}
        if interval==api_interval: return raw
        derived=aggregate_candles(raw,interval)
        with CANDLE_CACHE_LOCK:
            CANDLE_CACHE[derived_key]={'candles':derived,'ts':time.time()}
        return derived

# ================= GMAIL / RESEND SMTP EMAIL DISPATCH =================
import resend
resend.api_key = os.getenv("RESEND_API_KEY", "re_B1WFfX1Q_KnRJu6YitXAztBuLa6jFM1Ap")

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
LANDING_PAGE_HTML = '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="theme-color" content="#070d12"><title>StockFinder Pro | Market Intelligence</title><link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet"><style>\n:root{--bg:#070d12;--panel:#101b23;--line:#26343e;--gold:#f6aa2a;--green:#20cf83;--red:#ff5a65;--text:#eef4f7;--muted:#93a5b1}*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:radial-gradient(ellipse at 75% 10%,#12332d66,transparent 40%),var(--bg);font-family:\'Plus Jakarta Sans\',sans-serif;color:var(--text)}a{text-decoration:none;color:inherit}.wrap{max-width:1200px;margin:auto;padding:0 24px}header{position:sticky;top:0;z-index:10;background:#081017eb;border-bottom:1px solid var(--line);backdrop-filter:blur(16px)}header .wrap{min-height:76px;display:flex;align-items:center;justify-content:space-between;gap:16px}.brand{font-weight:800;letter-spacing:-.6px;font-size:19px;display:flex;align-items:center;gap:10px}.dot{height:11px;width:11px;border-radius:50%;background:var(--green);box-shadow:0 0 0 5px #20cf8318}.nav{display:flex;gap:10px;align-items:center}.btn{display:inline-flex;align-items:center;justify-content:center;gap:8px;padding:12px 20px;min-height:43px;border:1px solid #3a4a55;border-radius:10px;background:#14232c;font-size:13px;font-weight:800;transition:.2s}.btn:hover{transform:translateY(-2px);border-color:var(--gold)}.btn.gold{background:var(--gold);color:#171a1d;border-color:var(--gold)}.btn.ghost{background:transparent}.hero{padding:96px 0 65px;position:relative;overflow:hidden}.hero-grid{display:grid;grid-template-columns:1.05fr .95fr;align-items:center;gap:60px}.eyebrow{display:inline-flex;align-items:center;gap:9px;color:var(--green);background:#12352b;border:1px solid #285b49;border-radius:99px;padding:9px 14px;font-size:11px;font-weight:800;letter-spacing:1px;text-transform:uppercase}h1{font-size:clamp(42px,5.2vw,72px);line-height:1.07;letter-spacing:-3px;margin:24px 0}h1 em{font-style:normal;color:var(--gold)}.hero p{font-size:16px;line-height:1.85;color:var(--muted);max-width:530px}.actions{display:flex;gap:12px;flex-wrap:wrap;margin-top:28px}.stats{display:flex;gap:26px;margin-top:42px;flex-wrap:wrap}.stats strong{display:block;color:var(--text);font-size:22px}.stats small{color:var(--muted);font-size:11px}.preview{border:1px solid #39515c;border-radius:18px;background:#0b151d;box-shadow:0 35px 90px #0008;overflow:hidden;transform:perspective(1000px) rotateY(-3deg)}.preview-head{background:#101b23;border-bottom:1px solid var(--line);padding:15px 18px;display:flex;justify-content:space-between;align-items:center;font-weight:800;font-size:12px}.live{color:var(--green);font-size:10px}.preview-body{padding:18px}.mini-kpis{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}.mini-kpis div{padding:15px 9px;background:#14232c;border:1px solid var(--line);border-radius:9px;text-align:center}.mini-kpis small{display:block;color:var(--muted);font-size:9px;margin-bottom:9px}.mini-kpis b{font-size:20px}.green{color:var(--green)}.red{color:var(--red)}.heat{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-top:16px}.heat div{min-height:80px;border-radius:8px;padding:12px 8px;background:linear-gradient(140deg,#174638,#102820);border:1px solid #28684d;font-size:10px;font-weight:800;display:flex;flex-direction:column;justify-content:space-between}.heat div.down{background:linear-gradient(140deg,#502a35,#291b25);border-color:#713b49}.heat span{font-size:14px}.preview-foot{padding:14px 18px;color:var(--muted);font-size:10px;border-top:1px solid var(--line)}section{padding:74px 0;border-top:1px solid var(--line)}.kicker{font-size:11px;color:var(--gold);font-weight:800;letter-spacing:2px;text-transform:uppercase}.section-title{font-size:clamp(27px,3.5vw,41px);letter-spacing:-1.4px;margin:10px 0 14px}.section-desc{color:var(--muted);line-height:1.7;max-width:630px}.cards{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-top:32px}.card,.price{background:linear-gradient(145deg,#14232c,#0c151c);border:1px solid var(--line);border-radius:14px;padding:25px}.card .symbol{font-size:25px;color:var(--gold)}.card h3{font-size:17px;margin:17px 0 9px}.card p{font-size:13px;line-height:1.8;color:var(--muted)}.prices{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-top:32px}.price{display:flex;flex-direction:column;gap:12px}.price.featured{border-color:var(--gold);box-shadow:0 0 0 1px #f6aa2a44}.price h3{margin:0;font-size:16px}.price .amount{font-size:29px;font-weight:800}.price small{color:var(--muted)}.price ul{padding-left:18px;color:#c3d0d8;font-size:12px;line-height:2}.price .btn{margin-top:auto}.cta{padding:50px;border:1px solid #456056;border-radius:16px;background:linear-gradient(110deg,#12352d,#111e26);text-align:center}.cta p{color:var(--muted)}footer{padding:28px 0;border-top:1px solid var(--line);font-size:12px;color:var(--muted)}@media(max-width:900px){.hero-grid{grid-template-columns:1fr;gap:40px}.preview{transform:none}.prices{grid-template-columns:repeat(2,1fr)}}@media(max-width:600px){header .wrap{padding:0 14px;min-height:64px}.brand{font-size:14px}.nav{gap:5px}.nav .btn{padding:9px;font-size:11px;min-height:36px}.nav .plans{display:none}.wrap{padding:0 16px}.hero{padding:55px 0 40px}h1{font-size:43px;letter-spacing:-2px}.hero p{font-size:14px}.stats{gap:18px}.cards,.prices{grid-template-columns:1fr}.mini-kpis b{font-size:16px}.heat{gap:5px}.heat div{padding:9px 5px;min-height:68px;font-size:8px}.heat span{font-size:11px}section{padding:48px 0}.cta{padding:30px 18px}}\n</style></head><body><header><div class="wrap"><a href="/" class="brand"><i class="dot"></i> STOCKFINDER PRO</a><nav class="nav"><a class="btn ghost plans" href="#pricing">Plans</a><a class="btn ghost" href="/login">Login</a><a class="btn gold" href="/signup">Sign up</a></nav></div></header><main><div class="hero"><div class="wrap hero-grid"><div><div class="eyebrow">● Institutional-style market intelligence</div><h1>Trade the setup.<br><em>Not the noise.</em></h1><p>One focused terminal for intraday screening, sector momentum and institutional demand &amp; supply zones. Scan faster, analyze clearly, and execute with discipline.</p><div class="actions"><a class="btn gold" href="/signup">Get started <span>↗</span></a><a class="btn" href="/login">Open trading terminal →</a></div><div class="stats"><div><strong>4</strong><small>Zone patterns</small></div><div><strong>5</strong><small>Market universes</small></div><div><strong>Multi-TF</strong><small>Zone analysis</small></div></div></div><div class="preview"><div class="preview-head"><span>◉ STOCKFINDER / MARKET OVERVIEW</span><span class="live">● TERMINAL PREVIEW</span></div><div class="preview-body"><div class="mini-kpis"><div><small>LONG BUILDUP</small><b class="green">↗</b></div><div><small>SHORT BUILDUP</small><b class="red">↘</b></div><div><small>SUPPLY / DEMAND</small><b style="color:var(--gold)">◈</b></div></div><div class="heat"><div>NIFTY BANK<span class="green">BULLISH</span></div><div class="down">NIFTY IT<span class="red">BEARISH</span></div><div>NIFTY AUTO<span class="green">BULLISH</span></div><div class="down">NIFTY METAL<span class="red">BEARISH</span></div><div>NIFTY FMCG<span class="green">BULLISH</span></div><div>NIFTY ENERGY<span class="green">BULLISH</span></div><div class="down">FIN SERVICES<span class="red">BEARISH</span></div><div>REALTY<span class="green">BULLISH</span></div></div></div><div class="preview-foot">Illustrative interface only · Not live market signals</div></div></div></div><section id="features"><div class="wrap"><div class="kicker">ONE UNIFIED WORKSPACE</div><h2 class="section-title">Everything you need to find your edge.</h2><p class="section-desc">A clean, purpose-built interface that keeps your market workflow organized.</p><div class="cards"><article class="card"><div class="symbol">▥</div><h3>Live stock screener</h3><p>Filter by momentum, open-high/low patterns and price behavior across supported stock universes.</p></article><article class="card"><div class="symbol">◈</div><h3>Demand &amp; supply zones</h3><p>Explore fresh zones, structure and higher-timeframe context without switching between scattered tools.</p></article><article class="card"><div class="symbol">▦</div><h3>Sector heatmap</h3><p>Read sector strength, relative weakness and market breadth through intuitive green and red visuals.</p></article></div></div></section><section id="pricing"><div class="wrap"><div class="kicker">MEMBERSHIP</div><h2 class="section-title">Choose your access.</h2><p class="section-desc">Secure Razorpay checkout. Subscription activates only after successful payment verification.</p><div class="prices"><article class="price"><h3>Monthly</h3><div class="amount">₹499</div><small>1 month</small><ul><li>Trading terminal</li><li>Zone screener</li><li>Sector analysis</li></ul><a class="btn" href="/checkout?plan=monthly">Choose monthly</a></article><article class="price featured"><h3>Quarterly ★</h3><div class="amount">₹1,199</div><small>3 months</small><ul><li>Trading terminal</li><li>Zone screener</li><li>Sector analysis</li></ul><a class="btn gold" href="/checkout?plan=quarterly">Choose quarterly</a></article><article class="price"><h3>Half-yearly</h3><div class="amount">₹2,499</div><small>6 months</small><ul><li>Trading terminal</li><li>Zone screener</li><li>Sector analysis</li></ul><a class="btn" href="/checkout?plan=halfyearly">Choose half-yearly</a></article><article class="price"><h3>Yearly</h3><div class="amount">₹4,999</div><small>12 months</small><ul><li>Trading terminal</li><li>Zone screener</li><li>Sector analysis</li></ul><a class="btn" href="/checkout?plan=yearly">Choose yearly</a></article></div></div></section><section><div class="wrap"><div class="cta"><div class="kicker">START SCANNING</div><h2 class="section-title">Your next setup starts here.</h2><p>Focus on structure, confirmation and risk.</p><a class="btn gold" href="/signup">Create account →</a></div></div></section></main><footer><div class="wrap">© <span id="year"></span> StockFinder Pro · Market analysis tools are educational and do not guarantee trading outcomes.</div></footer><script>document.getElementById(\'year\').textContent=new Date().getFullYear()</script></body></html>'

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
  
/* Unified StockFinder Pro visual system */
:root{--bg:#070d12!important;--card:#101b23!important;--border:#30414c!important;--text:#c3d0d8!important;--muted:#93a5b1!important;--primary:#f6aa2a!important;--strong-text:#eef4f7!important;--red:#ff5a65!important}
body{min-height:100dvh!important;height:auto!important;background:radial-gradient(ellipse at 75% 0%,rgba(32,207,131,.12),transparent 45%),#070d12!important;padding:80px 16px 30px!important;color:#c3d0d8!important}
.login-card,.signup-card,.reset-card,.otp-card,.forgot-card{background:linear-gradient(145deg,#14232c,#0c151c)!important;border:1px solid #30414c!important;border-radius:18px!important;box-shadow:0 24px 65px #0006!important;padding:clamp(22px,5vw,42px)!important;max-width:440px!important}
.brand{color:#eef4f7!important;letter-spacing:-.5px}.brand:before{content:'●';color:#20cf83;margin-right:8px;font-size:13px}input{background:#0a141b!important;border:1px solid #344754!important;color:#eef4f7!important;min-height:46px!important}input:focus{border-color:#f6aa2a!important;box-shadow:0 0 0 3px #f6aa2a18!important}.btn-login,.btn-signup,button[type=submit]{background:#f6aa2a!important;color:#101820!important;border-radius:10px!important;min-height:46px!important}.btn-login:hover,.btn-signup:hover,button[type=submit]:hover{background:#ffbf53!important}.back-link,.forgot-link{color:#f6aa2a!important}@media(max-width:500px){body{padding:55px 14px 25px!important}.login-card,.signup-card{width:100%!important}}
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
  
/* Unified StockFinder Pro visual system */
:root{--bg:#070d12!important;--card:#101b23!important;--border:#30414c!important;--text:#c3d0d8!important;--muted:#93a5b1!important;--primary:#f6aa2a!important;--strong-text:#eef4f7!important;--red:#ff5a65!important}
body{min-height:100dvh!important;height:auto!important;background:radial-gradient(ellipse at 75% 0%,rgba(32,207,131,.12),transparent 45%),#070d12!important;padding:80px 16px 30px!important;color:#c3d0d8!important}
.login-card,.signup-card,.reset-card,.otp-card,.forgot-card{background:linear-gradient(145deg,#14232c,#0c151c)!important;border:1px solid #30414c!important;border-radius:18px!important;box-shadow:0 24px 65px #0006!important;padding:clamp(22px,5vw,42px)!important;max-width:440px!important}
.brand{color:#eef4f7!important;letter-spacing:-.5px}.brand:before{content:'●';color:#20cf83;margin-right:8px;font-size:13px}input{background:#0a141b!important;border:1px solid #344754!important;color:#eef4f7!important;min-height:46px!important}input:focus{border-color:#f6aa2a!important;box-shadow:0 0 0 3px #f6aa2a18!important}.btn-login,.btn-signup,button[type=submit]{background:#f6aa2a!important;color:#101820!important;border-radius:10px!important;min-height:46px!important}.btn-login:hover,.btn-signup:hover,button[type=submit]:hover{background:#ffbf53!important}.back-link,.forgot-link{color:#f6aa2a!important}@media(max-width:500px){body{padding:55px 14px 25px!important}.login-card,.signup-card{width:100%!important}}
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
  
/* Unified StockFinder Pro visual system */
:root{--bg:#070d12!important;--card:#101b23!important;--border:#30414c!important;--text:#c3d0d8!important;--muted:#93a5b1!important;--primary:#f6aa2a!important;--strong-text:#eef4f7!important;--red:#ff5a65!important}
body{min-height:100dvh!important;height:auto!important;background:radial-gradient(ellipse at 75% 0%,rgba(32,207,131,.12),transparent 45%),#070d12!important;padding:80px 16px 30px!important;color:#c3d0d8!important}
.login-card,.signup-card,.reset-card,.otp-card,.forgot-card{background:linear-gradient(145deg,#14232c,#0c151c)!important;border:1px solid #30414c!important;border-radius:18px!important;box-shadow:0 24px 65px #0006!important;padding:clamp(22px,5vw,42px)!important;max-width:440px!important}
.brand{color:#eef4f7!important;letter-spacing:-.5px}.brand:before{content:'●';color:#20cf83;margin-right:8px;font-size:13px}input{background:#0a141b!important;border:1px solid #344754!important;color:#eef4f7!important;min-height:46px!important}input:focus{border-color:#f6aa2a!important;box-shadow:0 0 0 3px #f6aa2a18!important}.btn-login,.btn-signup,button[type=submit]{background:#f6aa2a!important;color:#101820!important;border-radius:10px!important;min-height:46px!important}.btn-login:hover,.btn-signup:hover,button[type=submit]:hover{background:#ffbf53!important}.back-link,.forgot-link{color:#f6aa2a!important}@media(max-width:500px){body{padding:55px 14px 25px!important}.login-card,.signup-card{width:100%!important}}
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
  
/* Unified StockFinder Pro visual system */
:root{--bg:#070d12!important;--card:#101b23!important;--border:#30414c!important;--text:#c3d0d8!important;--muted:#93a5b1!important;--primary:#f6aa2a!important;--strong-text:#eef4f7!important;--red:#ff5a65!important}
body{min-height:100dvh!important;height:auto!important;background:radial-gradient(ellipse at 75% 0%,rgba(32,207,131,.12),transparent 45%),#070d12!important;padding:80px 16px 30px!important;color:#c3d0d8!important}
.login-card,.signup-card,.reset-card,.otp-card,.forgot-card{background:linear-gradient(145deg,#14232c,#0c151c)!important;border:1px solid #30414c!important;border-radius:18px!important;box-shadow:0 24px 65px #0006!important;padding:clamp(22px,5vw,42px)!important;max-width:440px!important}
.brand{color:#eef4f7!important;letter-spacing:-.5px}.brand:before{content:'●';color:#20cf83;margin-right:8px;font-size:13px}input{background:#0a141b!important;border:1px solid #344754!important;color:#eef4f7!important;min-height:46px!important}input:focus{border-color:#f6aa2a!important;box-shadow:0 0 0 3px #f6aa2a18!important}.btn-login,.btn-signup,button[type=submit]{background:#f6aa2a!important;color:#101820!important;border-radius:10px!important;min-height:46px!important}.btn-login:hover,.btn-signup:hover,button[type=submit]:hover{background:#ffbf53!important}.back-link,.forgot-link{color:#f6aa2a!important}@media(max-width:500px){body{padding:55px 14px 25px!important}.login-card,.signup-card{width:100%!important}}
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
  
/* Unified StockFinder Pro visual system */
:root{--bg:#070d12!important;--card:#101b23!important;--border:#30414c!important;--text:#c3d0d8!important;--muted:#93a5b1!important;--primary:#f6aa2a!important;--strong-text:#eef4f7!important;--red:#ff5a65!important}
body{min-height:100dvh!important;height:auto!important;background:radial-gradient(ellipse at 75% 0%,rgba(32,207,131,.12),transparent 45%),#070d12!important;padding:80px 16px 30px!important;color:#c3d0d8!important}
.login-card,.signup-card,.reset-card,.otp-card,.forgot-card{background:linear-gradient(145deg,#14232c,#0c151c)!important;border:1px solid #30414c!important;border-radius:18px!important;box-shadow:0 24px 65px #0006!important;padding:clamp(22px,5vw,42px)!important;max-width:440px!important}
.brand{color:#eef4f7!important;letter-spacing:-.5px}.brand:before{content:'●';color:#20cf83;margin-right:8px;font-size:13px}input{background:#0a141b!important;border:1px solid #344754!important;color:#eef4f7!important;min-height:46px!important}input:focus{border-color:#f6aa2a!important;box-shadow:0 0 0 3px #f6aa2a18!important}.btn-login,.btn-signup,button[type=submit]{background:#f6aa2a!important;color:#101820!important;border-radius:10px!important;min-height:46px!important}.btn-login:hover,.btn-signup:hover,button[type=submit]:hover{background:#ffbf53!important}.back-link,.forgot-link{color:#f6aa2a!important}@media(max-width:500px){body{padding:55px 14px 25px!important}.login-card,.signup-card{width:100%!important}}
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
        
        chosen_plan = request.form.get('plan') or session.get('selected_plan')
        session.clear()
        session.permanent = True
        session['user'] = email
        return redirect(url_for('checkout', plan=chosen_plan if chosen_plan in PLAN_DETAILS else 'monthly'))
    selected = request.args.get('plan', '')
    if selected in PLAN_DETAILS: session['selected_plan'] = selected
    else: session.pop('selected_plan', None)
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
                return redirect(url_for('terminal_app')) if active_subscription(user) else redirect(url_for('checkout', plan=chosen_plan if chosen_plan in PLAN_DETAILS else 'monthly'))
            
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
    return send_from_directory(basedir, 'index.html')

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
    user = current_user()
    if active_subscription(user):
        return redirect(url_for('terminal_app'))
    if not RAZORPAY_KEY_ID or not RAZORPAY_KEY_SECRET:
        app.logger.error('Razorpay credentials missing: configure both RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET')
        return ('Payment setup is incomplete. Please contact support (Razorpay credentials missing).', 503)
    amount, _ = PLAN_DETAILS[plan]
    try:
        r = requests.post('https://api.razorpay.com/v1/orders',
            auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET),
            json={'amount':amount,'currency':'INR','receipt':f'u{user.id}_{int(time.time())}',
                  'notes':{'plan':plan,'user_id':str(user.id)}}, timeout=20)
    except requests.RequestException:
        app.logger.exception('Razorpay network error during order creation')
        return ('Payment gateway is temporarily unreachable. Please retry in a moment.', 502)
    if not r.ok:
        try:
            gateway_error = r.json().get('error', {})
            reason = str(gateway_error.get('description') or gateway_error.get('code') or 'Unknown gateway error')[:250]
        except (ValueError, AttributeError, TypeError):
            reason = 'Unexpected gateway response'
        app.logger.error('Razorpay order rejected: HTTP %s; reason=%s', r.status_code, reason)
        if r.status_code in (401, 403):
            return ('Razorpay authentication failed. Check that the configured Key ID and Secret belong to the same test/live mode.', 503)
        return ('Payment order could not be created: ' + reason, 502)
    try:
        order = r.json()
        order_id = order['id']
        payment = Payment(user_id=user.id, order_id=order_id,plan=plan,amount_paise=amount)
        db.session.add(payment)
        db.session.commit()
    except Exception:
        db.session.rollback()
        app.logger.exception('Payment order created but local database save failed')
        return ('Payment order could not be saved. Please contact support.', 500)
    return render_template_string(CHECKOUT_HTML, key_id=RAZORPAY_KEY_ID,
        order_id=order['id'], amount=amount, plan=plan, email=current_user().email)

CHECKOUT_HTML = """<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Secure Checkout - Stock Finder Pro</title><script src="https://checkout.razorpay.com/v1/checkout.js"></script>
<style>body{background:radial-gradient(ellipse at 75% 0%,#12352d,#070d12 60%);color:#eef4f7;font-family:Plus Jakarta Sans,Arial,sans-serif;text-align:center;padding:70px 16px;min-height:100vh}button{background:#f6aa2a;color:#101820;font-weight:800;border:0;border-radius:9px;padding:14px 24px;font-weight:bold;cursor:pointer}a{color:#ff9e0b}</style></head>
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
    order_id=data.get('razorpay_order_id','rzp_test_TzOiKGfQnvAcaw')
    payment_id=data.get('razorpay_payment_id','RE9fr3o5YeqTo16lulirt0z5')
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
        force_refresh = request.args.get("refresh", "0") == "1"
        if SMART_ORB_SIGNALS and not force_refresh:
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
        if force_refresh:
            SMART_ORB_SIGNALS[:] = scored_stocks[:2]
        return jsonify({"status": "success", "timestamp": datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%H:%M:%S"), "data": scored_stocks[:2]})
    except Exception:
        return jsonify({"status": "success", "data": []})

ZONE_SCAN_LOCKS = {}
ZONE_SCAN_LOCKS_GUARD = threading.Lock()

# Progressive scan jobs: return immediately, stream completed batches via polling.
# Jobs are shared by all subscribers selecting the same universe/timeframe.
ZONE_JOBS = {}
ZONE_JOBS_LOCK = threading.RLock()
ZONE_JOB_TTL = 600
# Scans run before the user clicks; requests serve stored results immediately.
# Prewarm is opt-in on local Windows, so it does not spawn per Gunicorn worker.
PRECOMPUTE_UNIVERSES = tuple(x.strip().upper() for x in os.getenv('SFP_PRECOMPUTE_UNIVERSES', 'FO').split(',') if x.strip())
PRECOMPUTE_TIMEFRAMES = ('month', 'quarter', 'halfyear', 'day', 'week')
PRECOMPUTE_REFRESH_SECONDS = int(os.getenv('SFP_PRECOMPUTE_REFRESH_SECONDS', '21600'))
PRECOMPUTE_STOP = threading.Event()
PRECOMPUTE_THREAD = None

def _stored_zone_snapshot(univ_key, tf):
    try:
        with app.app_context():
            row = db.session.get(ZoneResultStore, f'v7_dr_rd_second_candle:{univ_key}:{tf}')
            if row is not None:
                data = json.loads(row.zones_json)
                if isinstance(data, list):
                    return data, row.updated_at
    except Exception as exc:
        print('Zone snapshot read:', type(exc).__name__, str(exc)[:120])
    return None, None

def _begin_zone_job(univ_key, tf, target_universe, force=False):
    key = (univ_key, tf)
    with ZONE_JOBS_LOCK:
        job = ZONE_JOBS.get(key)
        if job and not job.get('done'):
            return False
        if not force and job and time.time() - job['started'] < ZONE_JOB_TTL:
            return False
        quotes = QUOTE_CACHE.get(univ_key, {})
        live_quotes = {x['symbol']: x['ltp'] for x in quotes.get('data', [])} if time.time()-quotes.get('last_updated',0)<90 else {}
        ZONE_JOBS[key] = {'started': time.time(), 'completed': 0, 'total': len(target_universe), 'done': False, 'data': [], 'error': None}
        threading.Thread(target=_zone_scan_background, args=(key,tf,list(target_universe),live_quotes), daemon=True).start()
        return True

def _precompute_loop():
    # Complete all five TF scans once, then refresh in the background.
    # A cached snapshot remains visible while refresh is underway.
    while not PRECOMPUTE_STOP.is_set():
        for universe in PRECOMPUTE_UNIVERSES:
            try:
                univ_key, items = get_active_universe_list(universe)
            except Exception as exc:
                print('Precompute universe:', str(exc)[:120]); continue
            for tf in PRECOMPUTE_TIMEFRAMES:
                if PRECOMPUTE_STOP.is_set(): return
                _begin_zone_job(univ_key, tf, items, force=True)
                key = (univ_key, tf)
                while not PRECOMPUTE_STOP.wait(1.0):
                    with ZONE_JOBS_LOCK:
                        job = ZONE_JOBS.get(key)
                        if not job or job.get('done'): break
        PRECOMPUTE_STOP.wait(max(300, PRECOMPUTE_REFRESH_SECONDS))

def start_zone_precompute():
    global PRECOMPUTE_THREAD
    if PRECOMPUTE_THREAD and PRECOMPUTE_THREAD.is_alive(): return
    PRECOMPUTE_THREAD = threading.Thread(target=_precompute_loop, name='sfp-zone-precompute', daemon=True)
    PRECOMPUTE_THREAD.start()

def _zone_scan_background(key, tf, target_universe, live_quotes):
    started = time.time()
    results = []
    missing_history = 0
    univ_key = key[0]
    def worker(item):
        try:
            candles = fetch_historical_candles(item["isin"], tf)
            if not candles or len(candles) < 2: return None
            ltp = live_quotes.get(item["symbol"], max(candles, key=lambda c: str(c[0]))[4] if candles else 0)
            zones = scan_four_institutional_zones(candles, ltp, item["symbol"], tf)
            return [{
            "symbol": item["symbol"], "sector": item["sector"], "ltp": ltp,
            "pattern": z["pattern"], "bias": z["bias"], "proximal": z["proximal"], "distal": z["distal"],
            "entry": z["entry"], "stopLoss": z["stopLoss"], "risk": z["risk"], "riskPct": z["riskPct"],
            "target1R2": z["target1R2"], "target1R3": z["target1R3"],
            "strongZone": z.get("strongZone", False),
            "status": z["status"], "multiplier": z.get("multiplier", 1.0),
            "tvUrl": z["tvUrl"], "brokerUrl": z["brokerUrl"], "timeframe": z["timeframe"],
            "baseCandles": z.get("baseCandles"),
            "zoneCandleDate": z.get("zoneCandleDate"), "zoneCandleOpen": z.get("zoneCandleOpen"),
            "zoneCandleHigh": z.get("zoneCandleHigh"), "zoneCandleLow": z.get("zoneCandleLow"),
            "zoneCandleClose": z.get("zoneCandleClose"),
            "exitCandleDate": z.get("exitCandleDate"),
            "incomingCandleDate": z.get("incomingCandleDate"),
            "zoneBoundarySource": z.get("zoneBoundarySource")
            } for z in zones]
        except Exception as exc:
            print(f"Zone worker {tf}/{item.get('symbol')}: {type(exc).__name__}: {str(exc)[:150]}")
            return None


    try:
        # Controlled concurrency to reduce Upstox throttling. Publish partial results.
        with concurrent.futures.ThreadPoolExecutor(max_workers=int(os.getenv("SFP_ZONE_WORKERS", "6"))) as executor:
            futures = {executor.submit(worker, item): item for item in target_universe}
            for future in concurrent.futures.as_completed(futures):
                found = []
                try:
                    found = future.result()
                    if found is None:
                        missing_history += 1
                    elif found:
                        results.extend(found)
                except Exception as exc:
                    print('Zone worker error:', str(exc)[:180])
                with ZONE_JOBS_LOCK:
                    job = ZONE_JOBS.get(key)
                    if job:
                        job['completed'] += 1
                        if found:
                            job['data'].extend(found)
        # Do not replace a known-good snapshot with a severely incomplete scan.
        if target_universe and missing_history > len(target_universe) * 0.05:
            with ZONE_JOBS_LOCK:
                if key in ZONE_JOBS:
                    ZONE_JOBS[key].update(done=True, error=f"Historical data missing for {missing_history}/{len(target_universe)} stocks; previous snapshot preserved")
            print(f"Incomplete zone scan {univ_key}/{tf}: missing history for {missing_history}/{len(target_universe)}")
            return
        # Sort once at completion, after all stock requests finish.
        def cmp_distance(zone):
            cmp = float(zone.get("ltp") or 0)
            proximal = float(zone.get("proximal") or 0)
            distal = float(zone.get("distal") or 0)
            if cmp <= 0 or proximal <= 0 or distal <= 0:
                return float("inf")
            if min(proximal, distal) <= cmp <= max(proximal, distal):
                return 0.0
            if zone["bias"] == "DEMAND" and proximal <= cmp:
                return (cmp - proximal) / cmp * 100
            if zone["bias"] == "SUPPLY" and proximal >= cmp:
                return (proximal - cmp) / cmp * 100
            return float("inf")


        results.sort(key=lambda x: (cmp_distance(x), x.get('symbol', '')))
        try:
            with app.app_context():
                store_key = f'v7_dr_rd_second_candle:{univ_key}:{tf}'
                record = db.session.get(ZoneResultStore, store_key)
                if record is None:
                    record = ZoneResultStore(cache_key=store_key, zones_json='[]')
                    db.session.add(record)
                record.zones_json = json.dumps(results, separators=(',', ':'))
                record.updated_at = datetime.utcnow()
                db.session.commit()
        except Exception as exc:
            db.session.rollback()
            print('Zone result persistence:', type(exc).__name__, str(exc)[:120])
        with ZONE_CACHE_LOCK:
            entry = ZONE_CACHE.setdefault(univ_key, {'last_updated': {}})
            entry[tf] = results
            entry['last_updated'][tf] = time.time()
        with ZONE_JOBS_LOCK:
            if key in ZONE_JOBS:
                ZONE_JOBS[key].update(data=results, done=True, completed=len(target_universe))
        print(f'Zone scan completed {univ_key}/{tf}: {len(results)} zones in {time.time()-started:.1f}s')
    except Exception as exc:
        print('Zone background scan failed:', repr(exc))
        with ZONE_JOBS_LOCK:
            if key in ZONE_JOBS:
                ZONE_JOBS[key].update(done=True, error='Scan failed; check Render logs')

# Gunicorn imports this module rather than executing __main__. Enable only with
# SFP_PRECOMPUTE=1 and ONE Gunicorn worker; otherwise each worker starts its own scans.
if os.getenv('SFP_PRECOMPUTE', '0') == '1' and __name__ != '__main__':
    start_zone_precompute()

@app.route('/api/zone-screener', methods=['GET'])
def zone_screener():
    tf = request.args.get('tf', 'day').lower()
    if tf not in ('day','week','month','quarter','halfyear','year','5minute','15minute','30minute','60minute'):
        tf = 'day'
    univ_key, target_universe = get_active_universe_list()
    key = (univ_key, tf)
    force_refresh = request.args.get('refresh') == '1'
    with ZONE_CACHE_LOCK:
        cache = ZONE_CACHE.setdefault(univ_key, {'last_updated': {}})
        cached = cache.get(tf)
        age = time.time() - cache['last_updated'].get(tf, 0)
    if cached is not None and age < ZONE_JOB_TTL and not force_refresh:
        return jsonify(status='success', data=cached, done=True, completed=len(target_universe), total=len(target_universe), source='memory')
    persisted, updated_at = _stored_zone_snapshot(univ_key, tf)
    if persisted is not None and not force_refresh:
        stale = not updated_at or (datetime.utcnow()-updated_at).total_seconds() >= HISTORY_CACHE_SECONDS
        if stale:
            _begin_zone_job(univ_key, tf, target_universe)
        with ZONE_JOBS_LOCK:
            job = ZONE_JOBS.get(key)
            refreshing = bool(job and not job.get('done'))
        return jsonify(status='success', data=persisted, done=True, completed=len(target_universe), total=len(target_universe), source='database', stale=stale, refreshing=refreshing, updatedAt=updated_at.isoformat() if updated_at else None)
    _begin_zone_job(univ_key, tf, target_universe, force=force_refresh)
    with ZONE_JOBS_LOCK:
        job = ZONE_JOBS.get(key)
        if job:
            return jsonify(status='success', data=list(job['data']), done=job['done'], completed=job['completed'], total=job['total'], message=job.get('error'), source='scan')
    return jsonify(status='error', data=[], done=True, message='Scan could not start'), 503

@app.route('/api/zone-cache-status', methods=['GET'])
def zone_cache_status():
    # Used by local testing to confirm each timeframe is precomputed.
    universe = request.args.get('universe', 'FO').upper()
    if universe not in ('FO','NIFTY500','MIDCAP','SMALLCAP','TOTALMARKET'):
        return jsonify(status='error', message='Unknown universe'), 400
    output = {}
    for tf in PRECOMPUTE_TIMEFRAMES:
        rows, updated = _stored_zone_snapshot(universe, tf)
        with ZONE_JOBS_LOCK:
            job = ZONE_JOBS.get((universe, tf))
            progress = {'completed':job['completed'], 'total':job['total'], 'running':not job['done']} if job else None
        output[tf] = {'ready':rows is not None, 'zones':len(rows) if rows is not None else 0,
                      'updatedAt':updated.isoformat() if updated else None, 'scan':progress}
    return jsonify(status='success', universe=universe, timeframes=output)

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
    if os.getenv('SFP_PRECOMPUTE', '0') == '1':
        start_zone_precompute()
    port = int(os.environ.get('PORT', 5000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)

