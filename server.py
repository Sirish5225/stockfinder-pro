import os
import requests
import csv
import io
import gzip
import urllib.request
import time
from datetime import datetime, date
import pytz
import concurrent.futures
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from collections import defaultdict

app = Flask(__name__)
CORS(app)

UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiJDTDgwMDQiLCJqdGkiOiI2YTZlZGU0YTdkMDdkYzI0NTcxY2IwNjgiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlzRXh0ZW5kZWQiOnRydWUsImlhdCI6MTc4NTY1MDc2MiwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxODE3MjQ0MDAwfQ.QevX5BwRdiDzZNmuSc0CGqDZcN5VP1qK6GXbvziEAik").strip()

QUOTE_CACHE = {"data": [], "last_updated": 0}
ZONE_CACHE = {"day": [], "week": [], "month": [], "quarter": [], "halfyear": [], "last_updated": {}}

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
    "LTIM": "Nifty IT", "MPHASIS": "Nifty IT", "PERSISTENT": "Nifty IT", "TCS": "Nifty IT",
    "TECHM": "Nifty IT", "WIPRO": "Nifty IT",
    "ASHOKLEY": "Nifty Auto", "BAJAJ-AUTO": "Nifty Auto", "BALKRISIND": "Nifty Auto", "BOSCHLTD": "Nifty Auto",
    "EICHERMOT": "Nifty Auto", "FORCEMOT": "Nifty Auto", "HEROMOTOCO": "Nifty Auto", "HYUNDAI": "Nifty Auto",
    "M&M": "Nifty Auto", "MARUTI": "Nifty Auto", "MOTHERSON": "Nifty Auto", "SONACOMS": "Nifty Auto",
    "TATAMOTORS": "Nifty Auto", "TMPVL": "Nifty Auto", "TIINDIA": "Nifty Auto", "TVSMOTOR": "Nifty Auto",
    "UNOMINDA": "Nifty Auto",
    "ALKEM": "Nifty Pharma", "APOLLOHOSP": "Nifty Pharma", "AUROPHARMA": "Nifty Pharma", "BIOCON": "Nifty Pharma",
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
    "BAJAJFINSV": "Fin Service", "BAJAJHLDNG": "Fin Service", "BSE": "Fin Service", "CAMS": "Fin Service",
    "CDSL": "Fin Service", "CHOLAFIN": "Fin Service", "HDFCAMC": "Fin Service", "HDFCLIFE": "Fin Service",
    "ICICIGI": "Fin Service", "ICICIPRULI": "Fin Service", "IRFC": "Fin Service", "JIOFIN": "Fin Service",
    "KFINTECH": "Fin Service", "LICI": "Fin Service", "LTF": "Fin Service", "LICHSGFIN": "Fin Service",
    "MANAPPURAM": "Fin Service", "MFSL": "Fin Service", "MOTILALOFS": "Fin Service", "MCX": "Fin Service",
    "MUTHOOTFIN": "Fin Service", "NAM-INDIA": "Fin Service", "PAYTM": "Fin Service", "POLICYBZR": "Fin Service",
    "PNBHOUSING": "Fin Service", "PFC": "Fin Service", "RECLTD": "Fin Service", "SBICARD": "Fin Service",
    "SBILIFE": "Fin Service", "SHRIRAMFIN": "Fin Service", "OFSS": "Fin Service",
    "BRITANNIA": "Nifty FMCG", "COLPAL": "Nifty FMCG", "DABUR": "Nifty FMCG", "DMART": "Nifty FMCG",
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

def load_dynamic_universe():
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    universe = []
    seen = set()
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response:
            with gzip.open(response, 'rt', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader) 
                for row in reader:
                    if len(row) < 4: continue
                    inst_key, symbol, name = row[0].strip(), row[2].strip(), row[3].strip()
                    if inst_key.startswith("NSE_EQ|") and symbol in TARGET_FO_SYMBOLS and symbol not in seen:
                        seen.add(symbol)
                        universe.append({"symbol": symbol, "name": name, "sector": TARGET_FO_SYMBOLS[symbol], "isin": inst_key, "is_fo": True})
        return universe
    except Exception:
        return [{"symbol": "NIFTY", "name": "Fallback", "sector": "Error", "isin": "NSE_INDEX|Nifty 50", "is_fo": False}]

GLOBAL_MARKET_UNIVERSE = load_dynamic_universe()
CORE_MARKET_UNIVERSE = GLOBAL_MARKET_UNIVERSE.copy()

# =========================================================================
# REFINED INSTITUTIONAL DEMAND & SUPPLY ZONE SCANNER
# =========================================================================

def classify_candle(o, h, l, c):
    candle_range = h - l
    if candle_range == 0: return "BASE"
    body = abs(c - o)
    # A base candle body should be <= 55% of the total candle range
    if (body / candle_range) <= 0.55: return "BASE"
    return "RALLY" if c > o else "DROP"

def is_stronger_legout(leg_in, leg_out):
    """ Allows a 10% margin of error so visually strong explosive candles aren't falsely rejected by a single tick """
    body_in = abs(leg_in[4] - leg_in[1])
    body_out = abs(leg_out[4] - leg_out[1])
    range_in = leg_in[2] - leg_in[3]
    range_out = leg_out[2] - leg_out[3]
    return (body_out >= body_in * 0.90) and (range_out >= range_in * 0.90)

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

def scan_chart_for_active_zones(candles, current_ltp):
    if len(candles) < 6: return None
    
    valid_zones = []

    # Scan the entire chart backwards to collect ALL unviolated zones
    for i in range(len(candles) - 1, 2, -1):
        c_out = candles[i]
        t_out = classify_candle(*c_out[1:5])
        if t_out == "BASE": continue

        c_base1 = candles[i-1]
        t_base1 = classify_candle(*c_base1[1:5])
        c_in1 = candles[i-2]
        t_in1 = classify_candle(*c_in1[1:5])

        pattern, bias, bases = None, None, []
        proximal, distal = 0.0, 0.0

        if i >= 3:
            c_base2 = candles[i-1]
            c_base1_2 = candles[i-2]
            c_in2 = candles[i-3]
            t_base2 = classify_candle(*c_base2[1:5])
            t_base1_2 = classify_candle(*c_base1_2[1:5])
            t_in2 = classify_candle(*c_in2[1:5])

            if t_base1_2 == "BASE" and t_base2 == "BASE" and t_in2 in ["RALLY", "DROP"] and is_stronger_legout(c_in2, c_out):
                bases = [c_base1_2, c_base2]
                if t_in2 == "DROP" and t_out == "RALLY": pattern, bias = "D-B-B-R", "DEMAND"
                elif t_in2 == "RALLY" and t_out == "RALLY": pattern, bias = "R-B-B-R", "DEMAND"
                elif t_in2 == "RALLY" and t_out == "DROP": pattern, bias = "R-B-B-D", "SUPPLY"
                elif t_in2 == "DROP" and t_out == "DROP": pattern, bias = "D-B-B-D", "SUPPLY"

        if not pattern and t_base1 == "BASE" and t_in1 in ["RALLY", "DROP"] and is_stronger_legout(c_in1, c_out):
            bases = [c_base1]
            if t_in1 == "DROP" and t_out == "RALLY": pattern, bias = "D-B-R", "DEMAND"
            elif t_in1 == "RALLY" and t_out == "RALLY": pattern, bias = "R-B-R", "DEMAND"
            elif t_in1 == "RALLY" and t_out == "DROP": pattern, bias = "R-B-D", "SUPPLY"
            elif t_in1 == "DROP" and t_out == "DROP": pattern, bias = "D-B-D", "SUPPLY"

        if pattern and bases:
            if bias == "DEMAND":
                proximal = max(max(b[1], b[4]) for b in bases)
                distal = min(b[3] for b in bases)
            else:
                proximal = min(min(b[1], b[4]) for b in bases)
                distal = max(b[2] for b in bases)

        if not pattern:
            if t_base1 in ["RALLY", "DROP"] and t_base1 != t_out and is_stronger_legout(c_base1, c_out):
                leg_in, leg_out = c_base1, c_out
                if t_base1 == "DROP" and t_out == "RALLY": 
                    pattern, bias = "D-R (V)", "DEMAND"
                    proximal = max(leg_in[4], leg_out[1]) 
                    distal = min(leg_in[3], leg_out[3])
                elif t_base1 == "RALLY" and t_out == "DROP": 
                    pattern, bias = "R-D (V)", "SUPPLY"
                    proximal = min(leg_in[4], leg_out[1])
                    distal = max(leg_in[2], leg_out[2])

        if pattern and proximal > 0 and distal > 0 and proximal != distal:
            risk = round(abs(proximal - distal), 2)
            if risk <= 0: continue

            violated, tested = False, False
            for j in range(i + 1, len(candles)):
                c_high, c_low = candles[j][2], candles[j][3]
                if bias == "DEMAND":
                    if c_low < distal: violated = True; break
                    if c_low <= proximal: tested = True
                if bias == "SUPPLY":
                    if c_high > distal: violated = True; break
                    if c_high >= proximal: tested = True
            
            if not violated:
                status = "Tested" if tested else "Untested (Fresh)"
                if bias == "DEMAND":
                    if distal <= current_ltp <= proximal: status = "🎯 In Zone (Actionable)"
                    elif proximal < current_ltp <= proximal + (risk * 2): status = "⚡ Approaching H2"
                else:
                    if proximal <= current_ltp <= distal: status = "🎯 In Zone (Actionable)"
                    elif proximal - (risk * 2) <= current_ltp < proximal: status = "⚡ Approaching L2"

                valid_zones.append({
                    "pattern": pattern, "bias": bias, 
                    "proximal": round(proximal, 2), "distal": round(distal, 2), 
                    "risk": risk, "status": status,
                    "distance_to_ltp": abs(proximal - current_ltp)
                })
                
    if valid_zones:
        # Sort by closest to the current market price and return the best one
        valid_zones.sort(key=lambda x: x["distance_to_ltp"])
        best_zone = valid_zones[0]
        del best_zone["distance_to_ltp"]
        return best_zone

    return None

def fetch_historical_candles(instrument_key, interval):
    to_date = date.today().strftime('%Y-%m-%d')
    
    # Fix for API constraints: Daily limit is usually 1-3 years.
    if interval == "day": from_date = "2023-01-01"
    elif interval == "week": from_date = "2020-01-01"
    else: from_date = "2015-01-01"
        
    api_interval = "month" if interval in ["quarter", "halfyear"] else interval
    url = f"https://api.upstox.com/v2/historical-candle/{instrument_key}/{api_interval}/{to_date}/{from_date}"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}
    
    for _ in range(2):
        try:
            res = requests.get(url, headers=headers, timeout=6)
            if res.status_code == 200:
                candles = res.json().get("data", {}).get("candles", [])
                candles.reverse()
                if interval == "quarter": return aggregate_candles(candles, 3)
                elif interval == "halfyear": return aggregate_candles(candles, 6)
                return candles
            elif res.status_code == 429: time.sleep(0.4)
        except Exception: pass
    return []

@app.route("/api/zone-screener", methods=["GET"])
def zone_screener():
    global ZONE_CACHE
    tf = request.args.get("tf", "day").lower()
    if tf not in ["day", "week", "month", "quarter", "halfyear"]: tf = "day"

    if time.time() - ZONE_CACHE["last_updated"].get(tf, 0) < 900 and len(ZONE_CACHE.get(tf, [])) > 0:
        return jsonify({"status": "success", "data": ZONE_CACHE[tf]})

    live_quotes = {s["symbol"]: s["ltp"] for s in (fetch_live_upstox_quotes() if isinstance(fetch_live_upstox_quotes(), list) else [])}
    results = []

    def worker(item):
        candles = fetch_historical_candles(item["isin"], tf)
        if not candles: return None
        ltp = live_quotes.get(item["symbol"], candles[-1][4])
        zone = scan_chart_for_active_zones(candles, ltp)
        if zone:
            return {"symbol": item["symbol"], "sector": item["sector"], "ltp": ltp, "pattern": zone["pattern"], "bias": zone["bias"], "proximal": zone["proximal"], "distal": zone["distal"], "risk": zone["risk"], "status": zone["status"]}
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(worker, stock) for stock in GLOBAL_MARKET_UNIVERSE]
        for f in concurrent.futures.as_completed(futures):
            res = f.result()
            if res: results.append(res)

    results.sort(key=lambda x: (0 if "🎯" in x["status"] else 1 if "⚡" in x["status"] else 2, x["risk"]))
    
    if results:
        ZONE_CACHE[tf] = results
        ZONE_CACHE["last_updated"][tf] = time.time()
        
    return jsonify({"status": "success", "data": results})

# =========================================================================
# LIVE SCREENER & SECTOR APIS
# =========================================================================

def detect_open_setup(open_p, high_p, low_p):
    if open_p <= 0 or low_p <= 0 or high_p <= 0: return None
    if abs(open_p - low_p) <= 0.05 or abs(open_p - low_p) <= (open_p * 0.0005): return "OPEN=LOW"
    elif abs(open_p - high_p) <= 0.05 or abs(open_p - high_p) <= (open_p * 0.0005): return "OPEN=HIGH"
    return None

def calculate_oi_buildup(price_pct, oi_pct, is_fo, candle_tag):
    if not is_fo: return "Cash Equity"
    if candle_tag == "OPEN=HIGH" or price_pct <= -0.3: return "Short Buildup"
    elif candle_tag == "OPEN=LOW" or price_pct >= 0.3: return "Long Buildup"
    elif price_pct > 0: return "Short Covering"
    elif price_pct < 0: return "Long Unwinding"
    return "Neutral"

def compute_aggressive_momentum(price_pct, vol_spike, candle_tag):
    base_score = abs(price_pct) * 15.0
    spike_bonus = vol_spike * 10.0
    score = base_score + spike_bonus
    if candle_tag in ["OPEN=LOW", "OPEN=HIGH"]: score += 35.0
    return round(score, 1)

def fetch_live_upstox_quotes():
    global QUOTE_CACHE
    if not UPSTOX_ACCESS_TOKEN or UPSTOX_ACCESS_TOKEN == "YOUR_NEW_TOKEN_HERE":
        return {"error": "Invalid API Token."}

    if time.time() - QUOTE_CACHE["last_updated"] < 6 and QUOTE_CACHE["data"]:
        return QUOTE_CACHE["data"]

    results = []
    chunk_size = 500 
    headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}

    for i in range(0, len(GLOBAL_MARKET_UNIVERSE), chunk_size):
        chunk = GLOBAL_MARKET_UNIVERSE[i:i + chunk_size]
        keys_param = ",".join([item["isin"] for item in chunk])
        url = f"https://api.upstox.com/v2/market-quote/quotes?instrument_key={keys_param}"

        try:
            res = requests.get(url, headers=headers, timeout=10)
            if res.status_code == 401: return {"error": "Upstox API Token Expired."}
            elif res.status_code != 200: continue
            quotes_data = res.json().get("data", {})

            for item in chunk:
                sym = item["symbol"]
                isin_raw = item["isin"]
                isin_colon = isin_raw.replace("|", ":")

                q = quotes_data.get(isin_raw) or quotes_data.get(isin_colon) or quotes_data.get(f"NSE_EQ:{sym}") or quotes_data.get(sym)
                if not q: continue

                ltp = float(q.get("last_price", 0.0))
                ohlc = q.get("ohlc", {})
                open_p = float(ohlc.get("open") or 0.0)
                low_p = float(ohlc.get("low") or 0.0)
                high_p = float(ohlc.get("high") or 0.0)
                volume = int(q.get("volume", 0))

                average_volume = int(q.get("average_volume") or q.get("avg_volume") or (volume * 0.6) or 100000)
                net_change = float(q.get("net_change") or 0.0)
                prev_close = float(q.get("prev_close") or q.get("prev_close_price") or 0.0)
                if prev_close == 0.0 and (ltp - net_change) > 0: prev_close = ltp - net_change

                price_pct = round(((ltp - prev_close) / prev_close) * 100, 2) if prev_close > 0 else 0.0
                candle_tag = detect_open_setup(open_p, high_p, low_p)
                oi = float(q.get("oi") or 0.0)
                prev_oi = float(q.get("prev_oi") or 0.0)
                oi_pct = round(((oi - prev_oi) / prev_oi) * 100, 2) if (oi > 0 and prev_oi > 0) else abs(price_pct) * 1.25
                vol_spike = volume / average_volume if average_volume > 0 else 1.0

                results.append({
                    "symbol": sym, "name": item["name"], "sector": item.get("sector", "F&O Universe"),
                    "ltp": ltp, "pricePct": price_pct, "volume": volume, "volMultiplier": round(vol_spike, 2),
                    "oiPct": oi_pct, "buildup": calculate_oi_buildup(price_pct, oi_pct, True, candle_tag),
                    "momentumScore": compute_aggressive_momentum(price_pct, vol_spike, candle_tag),
                    "candleTag": candle_tag, "is_fo": True
                })
        except Exception:
            pass

    if results:
        results.sort(key=lambda x: x["momentumScore"], reverse=True)
        QUOTE_CACHE["data"] = results
        QUOTE_CACHE["last_updated"] = time.time()
        return results
    return {"error": "No market data retrieved."}

@app.route("/")
def index():
    return send_from_directory('.', 'index.html')

@app.route("/api/upload-universe", methods=["POST"])
def upload_universe():
    global GLOBAL_MARKET_UNIVERSE
    if 'file' not in request.files: return jsonify({"status": "error", "message": "No file uploaded"}), 400
    file = request.files['file']
    if file.filename == '': return jsonify({"status": "error", "message": "Empty file name"}), 400

    try:
        stream = io.TextIOWrapper(file.stream, encoding="utf-8-sig")
        sample_line = stream.readline()
        stream.seek(0)
        existing_isin_map = {item["symbol"]: item for item in CORE_MARKET_UNIVERSE}
        new_universe = []
        
        if ',' not in sample_line:
            reader = csv.reader(stream)
            for row in reader:
                if not row or not row[0].strip() or row[0].strip().upper() in ["SYMBOL", "TICKER", "STOCK"]: continue
                sym = row[0].strip().upper()
                if sym in existing_isin_map: new_universe.append(existing_isin_map[sym])
                else: new_universe.append({"symbol": sym, "name": f"{sym} Stock", "sector": "F&O Universe", "isin": f"NSE_EQ|{sym}", "is_fo": True})
        else:
            reader = csv.DictReader(stream)
            for row in reader:
                cleaned_row = {k.strip().lower(): v.strip() for k, v in row.items() if k is not None}
                sym = cleaned_row.get("symbol") or cleaned_row.get("ticker") or cleaned_row.get("stock")
                if not sym: continue
                sym = sym.upper()
                if sym in existing_isin_map:
                    item = existing_isin_map[sym].copy()
                    if cleaned_row.get("sector"): item["sector"] = cleaned_row.get("sector")
                    new_universe.append(item)
                else:
                    new_universe.append({"symbol": sym, "name": cleaned_row.get("name") or sym, "sector": cleaned_row.get("sector") or "F&O Universe", "isin": cleaned_row.get("isin") or f"NSE_EQ|{sym}", "is_fo": True})
            
        if new_universe:
            GLOBAL_MARKET_UNIVERSE = new_universe
            return jsonify({"status": "success", "message": f"Successfully loaded {len(new_universe)} stocks into screener universe!"})
        else:
            return jsonify({"status": "error", "message": "No valid rows found in the uploaded file."}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/reset-universe", methods=["POST"])
def reset_universe():
    global GLOBAL_MARKET_UNIVERSE
    GLOBAL_MARKET_UNIVERSE = CORE_MARKET_UNIVERSE.copy()
    return jsonify({"status": "success", "message": f"Successfully reset to default universe ({len(GLOBAL_MARKET_UNIVERSE)} stocks)!"})

@app.route("/api/screener", methods=["GET"])
def screener():
    filter_val = request.args.get("filter", "ALL")
    search_val = request.args.get("search", "").strip().lower()
    stocks = fetch_live_upstox_quotes()
    
    if isinstance(stocks, dict) and "error" in stocks: return jsonify({"status": "error", "message": stocks["error"]})

    top_gainers = [s for s in sorted(stocks, key=lambda x: float(x.get("pricePct", 0.0)), reverse=True) if float(s.get("pricePct", 0.0)) > 0][:20]
    top_losers = [s for s in sorted(stocks, key=lambda x: float(x.get("pricePct", 0.0))) if float(s.get("pricePct", 0.0)) < 0][:20]

    if filter_val and filter_val != "ALL":
        if filter_val in ["OPEN=LOW", "OPEN=HIGH"]: stocks = [x for x in stocks if x.get("candleTag") == filter_val]
        elif filter_val == "VOL_SHOCKER": stocks = [x for x in stocks if x.get("volMultiplier", 0) >= 1.5]
        else: stocks = [x for x in stocks if x.get("buildup", "").lower() == filter_val.lower()]

    if search_val: stocks = [x for x in stocks if search_val in x["symbol"].lower() or search_val in x["name"].lower()]

    return jsonify({
        "status": "success", "timestamp": datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%H:%M:%S"),
        "count": len(stocks), "data": stocks, "topGainers": top_gainers, "topLosers": top_losers
    })

@app.route("/api/sectors", methods=["GET"])
def get_sectors():
    stocks = fetch_live_upstox_quotes()
    if isinstance(stocks, dict) and "error" in stocks: return jsonify({"status": "error", "message": stocks["error"]})

    fo_stocks = [s for s in stocks if s.get("is_fo", True)]
    sec_map = defaultdict(lambda: {"stocks": 0, "total_pct": 0.0, "total_vol": 0, "advances": 0, "declines": 0})
    total_market_vol = total_market_advances = total_market_declines = 0

    for s in fo_stocks:
        sec = s.get("sector", "F&O Universe")
        p_pct, vol = float(s.get("pricePct", 0.0)), int(s.get("volume", 0))
        total_market_vol += vol
        if p_pct >= 0: total_market_advances += 1
        else: total_market_declines += 1

        sec_map[sec]["stocks"] += 1
        sec_map[sec]["total_pct"] += p_pct
        sec_map[sec]["total_vol"] += vol
        if p_pct >= 0: sec_map[sec]["advances"] += 1
        else: sec_map[sec]["declines"] += 1

    summary = [{
        "sector": sec, "avgChange": round(val["total_pct"] / val["stocks"], 2) if val["stocks"] > 0 else 0.0,
        "stocksCount": val["stocks"], "advances": val["advances"], "declines": val["declines"], "totalVolume": val["total_vol"]
    } for sec, val in sec_map.items()]

    summary.sort(key=lambda x: x["avgChange"], reverse=True)
    return jsonify({
        "status": "success", "data": summary,
        "marketBreadth": {"totalVolume": total_market_vol, "advances": total_market_advances, "declines": total_market_declines}
    })

@app.route("/api/ticker-bar", methods=["GET"])
def ticker_bar():
    headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}
    ticker_results = []
    index_isins = [idx["isin"] for idx in MARKET_INDICES]
    url = f"https://api.upstox.com/v2/market-quote/quotes?instrument_key={','.join(index_isins)}"
    
    try:
        res = requests.get(url, headers=headers, timeout=5)
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
    except Exception: pass

    stocks = fetch_live_upstox_quotes()
    if isinstance(stocks, list) and len(stocks) > 0:
        sorted_by_gain = sorted(stocks, key=lambda x: float(x.get("pricePct", 0.0)), reverse=True)
        if len(sorted_by_gain) > 0:
            top_gainer = sorted_by_gain[0]
            ticker_results.append({"symbol": f"TOP GAINER: {top_gainer['symbol']}", "price": f"{top_gainer['ltp']:,.2f}", "change": f"{top_gainer['pricePct']:+.2f}%", "isPositive": True})
            top_loser = sorted_by_gain[-1]
            ticker_results.append({"symbol": f"TOP LOSER: {top_loser['symbol']}", "price": f"{top_loser['ltp']:,.2f}", "change": f"{top_loser['pricePct']:+.2f}%", "isPositive": False})

    return jsonify({"status": "success", "data": ticker_results})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
