import os
import time
from datetime import datetime, date, timedelta
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

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from flask import Flask, jsonify, request, send_from_directory, render_template_string, redirect, url_for, session
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
CORS(app)
app.secret_key = "stock_finder_pro_secret_key"

# Absolute Path Fix for SQLite Database to prevent 'no such table' errors
basedir = os.path.abspath(os.path.dirname(__file__))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'users.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# User Database Model
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password = db.Column(db.String(200), nullable=False)
    watchlist = db.Column(db.Text, default="")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# Explicit Application Context with Guaranteed Table Creation
with app.app_context():
    try:
        db.create_all()
        print("✅ Database tables created and verified successfully at:", os.path.join(basedir, 'users.db'))
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

UPSTOX_ACCESS_TOKEN = os.getenv(
    "UPSTOX_ACCESS_TOKEN",
    "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiJDTDgwMDQiLCJqdGkiOiI2YTZlZGU0YTdkMDdkYzI0NTcxY2IwNjgiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlzRXh0ZW5kZWQiOnRydWUsImlhdCI6MTc4NTY1MDc2MiwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxODE3MjQ0MDAwfQ.QevX5BwRdiDzZNmuSc0CGqDZcN5VP1qK6GXbvziEAik"
).strip()

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
    "PETRONET": "Nifty Energy",
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
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
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

# ================= STRICT INSTITUTIONAL D&S ZONES ENGINE =================
def aggregate_candles(candles, factor):
    aggregated = []
    for i in range(0, len(candles), factor):
        chunk = candles[i:i+factor]
        if not chunk: continue
        o = chunk[0][1]
        h = max(c[2] for c in chunk)
        l = min(c[3] for c in chunk)
        c_close = chunk[-1][4]
        vol = sum(c[5] for c in chunk)
        aggregated.append([chunk[0][0], o, h, l, c_close, vol])
    return aggregated

def scan_strict_multitimeframe_zones(candles, current_ltp, symbol, tf):
    zones = []
    if len(candles) < 15: return zones

    for num_bases in [1, 2]:
        for i in range(num_bases, len(candles) - 2):
            legin_idx = i - num_bases
            base_start_idx = i
            base_end_idx = i + num_bases - 1
            legout_idx = base_end_idx + 1

            if legin_idx < 0 or legout_idx >= len(candles): continue

            legin_o, legin_c = candles[legin_idx][1], candles[legin_idx][4]
            legout_o, legout_c = candles[legout_idx][1], candles[legout_idx][4]

            legin_body = abs(legin_c - legin_o)
            legout_body = abs(legout_c - legout_o)

            base_candles = candles[base_start_idx : base_end_idx + 1]
            all_bases_valid = True
            
            for bc in base_candles:
                bc_o, bc_c = bc[1], bc[4]
                bc_range = bc[2] - bc[3]
                bc_body = abs(bc_c - bc_o)
                if bc_range <= 0 or (bc_body > bc_range * 0.25):
                    all_bases_valid = False
                    break

            if not all_bases_valid: continue

            max_base_body = max(abs(bc[4] - bc[1]) for bc in base_candles)
            if not (legin_body >= (max_base_body * 1.5) and legout_body > (legin_body * 1.2)): continue

            pattern, bias, proximal, distal = None, None, 0.0, 0.0

            if legout_c > legout_o:
                bias = "DEMAND"
                pattern = "Drop-Base-Rally" if legin_c < legin_o else "Rally-Base-Rally"
                proximal = max(max(bc[1], bc[4]) for bc in base_candles)
                distal = min(bc[3] for bc in base_candles)
            elif legout_c < legout_o:
                bias = "SUPPLY"
                pattern = "Rally-Base-Drop" if legin_c > legin_o else "Drop-Base-Drop"
                proximal = min(min(bc[1], bc[4]) for bc in base_candles)
                distal = max(bc[2] for bc in base_candles)

            if not pattern or proximal <= 0 or distal <= 0 or proximal == distal: continue
            risk = round(abs(proximal - distal), 2)
            if risk <= 0: continue

            status = "Untested (Fresh)"
            if bias == "DEMAND":
                if distal <= current_ltp <= proximal: status = "🎯 In Zone (Actionable)"
                elif current_ltp < distal: status = "Violated / Broken"
            else:
                if proximal <= current_ltp <= distal: status = "🎯 In Zone (Actionable)"
                elif current_ltp > distal: status = "Violated / Broken"

            if "Violated" in status: continue

            zones.append({
                "pattern": pattern, "bias": bias, "proximal": round(proximal, 2), "distal": round(distal, 2),
                "risk": risk, "target1R2": round(proximal + (risk * 2.0) if bias == 'DEMAND' else proximal - (risk * 2.0), 2),
                "target1R3": round(proximal + (risk * 3.0) if bias == 'DEMAND' else proximal - (risk * 3.0), 2),
                "status": status, "multiplier": round(legout_body / max(legin_body, 0.01), 2),
                "tvUrl": f"https://www.tradingview.com/chart/?symbol=NSE:{symbol}",
                "brokerUrl": f"https://groww.in/stocks/{symbol.lower()}",
                "timeframe": tf.upper()
            })

    return zones

def fetch_historical_candles(instrument_key, interval):
    global CANDLE_CACHE
    api_interval = "month" if interval in ["quarter", "halfyear"] else interval
    cache_key = f"{instrument_key}_{interval}"
    cached = CANDLE_CACHE.get(cache_key)
    if cached and (time.time() - cached["ts"] < 900) and cached["candles"]: return cached["candles"]

    to_date = date.today()
    from_date = to_date - timedelta(days=10 if interval == "5minute" else (30 if interval in ["15minute","30minute","60minute"] else (730 if interval == "day" else 1825)))
    url = f"https://api.upstox.com/v2/historical-candle/{urllib.parse.quote(instrument_key, safe='')}/{api_interval}/{to_date.strftime('%Y-%m-%d')}/{from_date.strftime('%Y-%m-%d')}"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}

    for attempt in range(3):
        try:
            res = HTTP_SESSION.get(url, headers=headers, timeout=6)
            if res.status_code == 200:
                candles = res.json().get("data", {}).get("candles", [])
                candles.reverse()
                if interval == "quarter": candles = aggregate_candles(candles, 3)
                elif interval == "halfyear": candles = aggregate_candles(candles, 6)
                if candles: CANDLE_CACHE[cache_key] = {"candles": candles, "ts": time.time()}
                return candles
            elif res.status_code == 429: time.sleep(0.3 * (attempt + 1))
        except Exception:
            time.sleep(0.2)
    return []

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

# ================= LANDING PAGE & AUTH TEMPLATES =================
LANDING_PAGE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Stock Finder Pro - Institutional Supply & Demand Screener</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #0a0a0c; --card: rgba(24, 24, 28, 0.7); --border: rgba(255, 255, 255, 0.08);
      --text: #e0e0e0; --muted: #8b8b93; --green: #2ecc71; --red: #e74c3c; --primary: #f39c12;
      --strong-text: #ffffff; --hover-bg: rgba(255, 255, 255, 0.03);
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Plus Jakarta Sans', sans-serif; }
    body { background-color: var(--bg); color: var(--text); overflow-x: hidden; }
    header { display: flex; justify-content: space-between; align-items: center; padding: 1.5rem 5%; border-bottom: 1px solid var(--border); background: rgba(10, 10, 12, 0.8); backdrop-filter: blur(12px); position: sticky; top: 0; z-index: 100; }
    .brand { font-weight: 800; font-size: 1.25rem; display: flex; align-items: center; gap: 10px; color: var(--primary); letter-spacing: -0.5px; }
    .dot { width: 10px; height: 10px; background: var(--green); border-radius: 50%; box-shadow: 0 0 10px var(--green); }
    .nav-cta { background: linear-gradient(135deg, #f39c12, #e67e22); color: #fff; padding: 0.7rem 1.4rem; border-radius: 10px; font-weight: 700; text-decoration: none; font-size: 0.85rem; box-shadow: 0 4px 20px rgba(243, 156, 18, 0.3); transition: transform 0.2s; }
    .nav-cta:hover { transform: translateY(-2px); }
    .hero { text-align: center; padding: 6rem 5% 4rem; max-width: 900px; margin: 0 auto; }
    .badge-pill { display: inline-flex; align-items: center; gap: 6px; background: rgba(243, 156, 18, 0.1); border: 1px solid rgba(243, 156, 18, 0.3); color: var(--primary); padding: 6px 14px; border-radius: 99px; font-size: 0.75rem; font-weight: 700; margin-bottom: 1.5rem; text-transform: uppercase; }
    h1 { font-size: 3.2rem; font-weight: 800; color: var(--strong-text); line-height: 1.2; margin-bottom: 1.5rem; letter-spacing: -1px; }
    h1 span { color: var(--primary); text-shadow: 0 0 30px rgba(243, 156, 18, 0.4); }
    p.hero-sub { font-size: 1.1rem; color: var(--muted); line-height: 1.6; margin-bottom: 2.5rem; max-width: 700px; margin-left: auto; margin-right: auto; }
    .cta-group { display: flex; gap: 1rem; justify-content: center; align-items: center; margin-bottom: 4rem; }
    .btn-primary { background: var(--primary); color: #000; padding: 0.9rem 2rem; border-radius: 12px; font-weight: 700; text-decoration: none; font-size: 0.95rem; box-shadow: 0 0 25px rgba(243, 156, 18, 0.4); transition: all 0.2s; }
    .btn-primary:hover { background: #e67e22; transform: translateY(-2px); }
    .btn-secondary { background: var(--card); border: 1px solid var(--border); color: var(--strong-text); padding: 0.9rem 2rem; border-radius: 12px; font-weight: 600; text-decoration: none; font-size: 0.95rem; transition: all 0.2s; }
    .btn-secondary:hover { border-color: var(--primary); background: var(--hover-bg); }
    .preview-card { background: var(--card); border: 1px solid var(--border); border-radius: 16px; padding: 1rem; box-shadow: 0 20px 50px rgba(0,0,0,0.6); max-width: 1000px; margin: 0 auto 6rem; backdrop-filter: blur(12px); }
    .preview-header { display: flex; gap: 6px; margin-bottom: 1rem; padding-left: 0.5rem; }
    .mac-dot { width: 10px; height: 10px; border-radius: 50%; }
    .dot-r { background: #e74c3c; } .dot-y { background: #f1c40f; } .dot-g { background: #2ecc71; }
    .preview-img-box { background: #0f0f12; border-radius: 10px; border: 1px solid var(--border); padding: 2rem; text-align: center; font-family: 'JetBrains Mono', monospace; color: var(--muted); font-size: 0.9rem; }
    footer { text-align: center; padding: 3rem; border-top: 1px solid var(--border); color: var(--muted); font-size: 0.8rem; margin-top: 4rem; }
  </style>
</head>
<body>
  <header>
    <div class="brand"><div class="dot"></div> STOCK FINDER PRO</div>
    <div style="display: flex; gap: 10px;">
      <a href="/login" class="nav-cta" style="background: transparent; border: 1px solid var(--border);">Login</a>
      <a href="/signup" class="nav-cta">Sign Up 🚀</a>
    </div>
  </header>
  <section class="hero">
    <div class="badge-pill">⚡ Strict Institutional Order Flow Screener</div>
    <h1>Scan Indian Equities Like <span>Smart Money</span></h1>
    <p class="hero-sub">Automatically detect strict unviolated institutional Demand & Supply zones with high-precision Doji-base rules.</p>
    <div class="cta-group">
      <a href="/signup" class="btn-primary">Get Started Free 🚀</a>
      <a href="/login" class="btn-secondary">Login to Terminal 🔐</a>
    </div>
  </section>
  <div class="preview-card">
    <div class="preview-header">
      <div class="mac-dot dot-r"></div>
      <div class="mac-dot dot-y"></div>
      <div class="mac-dot dot-g"></div>
    </div>
    <div class="preview-img-box">
      📊 Live NSE/BSE F&O Scanner • Strict Multi-Timeframe D&S Zone Detector Active
    </div>
  </div>
  <footer>
    &copy; 2026 Stock Finder Pro Terminal. Built for high-performance intraday and swing trading.
  </footer>
</body>
</html>
"""

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
    <form action="/signup" method="POST">
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

# ================= FLASK ROUTING & WATCHLIST API =================
@app.route("/")
def landing_page():
    return render_template_string(LANDING_PAGE_HTML)

@app.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "POST":
        email = request.form.get("email")
        password = request.form.get("password")
        
        user_exists = User.query.filter_by(email=email).first()
        if user_exists:
            return render_template_string(SIGNUP_PAGE_HTML, error="Email already registered! Please login.")
        
        hashed_password = generate_password_hash(password, method='pbkdf2:sha256')
        new_user = User(email=email, password=hashed_password, watchlist="")
        db.session.add(new_user)
        db.session.commit()
        
        session['user'] = email
        return redirect(url_for('terminal_app'))
    return render_template_string(SIGNUP_PAGE_HTML)

@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "POST":
        email = request.form.get("email")
        password = request.form.get("password")
        
        user = User.query.filter_by(email=email).first()
        if user and check_password_hash(user.password, password):
            session['user'] = email
            return redirect(url_for('terminal_app'))
        return render_template_string(LOGIN_PAGE_HTML, error="Invalid email or password!")
    return render_template_string(LOGIN_PAGE_HTML)

@app.route("/terminal")
def terminal_app():
    if 'user' not in session:
        return redirect(url_for('login_page'))
    return send_from_directory('.', 'index.html')

@app.route("/logout")
def logout():
    session.pop('user', None)
    return redirect(url_for('landing_page'))

# --- SERVER-SIDE WATCHLIST ENDPOINTS ---
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

# ================= ORIGINAL SCREENER & ZONE ENDPOINTS =================
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
    try:
        global ZONE_CACHE
        tf = request.args.get("tf", "day").lower()
        force_refresh = request.args.get("t") is not None
        univ_key, target_universe = get_active_universe_list()
        
        if tf not in ["5minute", "15minute", "30minute", "60minute", "day", "week", "month", "quarter", "halfyear"]: 
            tf = "day"
            
        univ_zone_cache = ZONE_CACHE.setdefault(univ_key, {"last_updated": {}})
        if not force_refresh and (time.time() - univ_zone_cache["last_updated"].get(tf, 0) < 120) and len(univ_zone_cache.get(tf, [])) > 0:
            return jsonify({"status": "success", "data": univ_zone_cache[tf]})

        quotes_res = fetch_live_upstox_quotes(univ_key)
        live_quotes = {s["symbol"]: s["ltp"] for s in (quotes_res if isinstance(quotes_res, list) else [])}
        results = []

        active_sample = target_universe[:75] 

        def worker(item):
            try:
                candles = fetch_historical_candles(item["isin"], tf)
                if not candles or len(candles) < 10: 
                    return []
                ltp = live_quotes.get(item["symbol"], candles[-1][4] if candles else 0)
                zones = scan_strict_multitimeframe_zones(candles, ltp, item["symbol"], tf)
                return [{
                    "symbol": item["symbol"], "sector": item["sector"], "ltp": ltp,
                    "pattern": z["pattern"], "bias": z["bias"], "proximal": z["proximal"], "distal": z["distal"],
                    "risk": z["risk"], "risk_pct": round((z["risk"] / ltp) * 100, 2) if ltp > 0 else 0,
                    "target1R2": z["target1R2"], "target1R3": z["target1R3"],
                    "status": z["status"], "multiplier": z.get("multiplier", 1.0),
                    "tvUrl": z["tvUrl"], "brokerUrl": z["brokerUrl"], "timeframe": z["timeframe"]
                } for z in zones]
            except Exception:
                return []

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            for res in executor.map(worker, active_sample):
                if res: 
                    results.extend(res)

        results.sort(key=lambda x: (0 if x["bias"] == "DEMAND" else 1, -x.get("multiplier", 1.0)))
        
        univ_zone_cache[tf] = results
        univ_zone_cache["last_updated"][tf] = time.time()
        return jsonify({"status": "success", "data": results})
    except Exception as e:
        print("Zone Screener Error:", e)
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
