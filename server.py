import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import csv
import io
import gzip
import urllib.request
import urllib.parse
import time
from datetime import datetime, date, timedelta
import pytz
import concurrent.futures
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from collections import defaultdict
import threading

app = Flask(__name__)
CORS(app)

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
    "NIFTY500": {"data": [], "last_updated": 0}
}
ZONE_CACHE = {
    "FO": {"last_updated": {}},
    "NIFTY500": {"last_updated": {}}
}
CANDLE_CACHE = {}

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
    "LTIM": "Nifty IT", "CAMS": "Nifty IT", "MPHASIS": "Nifty IT", "PERSISTENT": "Nifty IT", "TCS": "Nifty IT",
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
    "BAJAJFINSV": "Fin Service", "BAJAJHLDNG": "Fin Service", "BSE": "Fin Service", 
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
    if universe: NIFTY500_UNIVERSE_CACHE = universe
    return universe or CORE_MARKET_UNIVERSE

def get_active_universe_list(univ_type=None):
    u = (univ_type or request.args.get("universe", "FO")).upper()
    return ("NIFTY500", load_nifty500_universe()) if u == "NIFTY500" else ("FO", GLOBAL_MARKET_UNIVERSE)

def detect_open_setup(open_p, high_p, low_p):
    if open_p <= 0 or low_p <= 0 or high_p <= 0: return None
    if abs(open_p - low_p) <= 0.05 or abs(open_p - low_p) <= (open_p * 0.0005): return "OPEN=LOW"
    elif abs(open_p - high_p) <= 0.05 or abs(open_p - high_p) <= (open_p * 0.0005): return "OPEN=HIGH"
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
                chunk_results.append({
                    "symbol": sym, "name": item["name"], "sector": item.get("sector", "NIFTY 500"),
                    "ltp": ltp, "pricePct": price_pct, "volume": volume, "volMultiplier": round(vol_spike, 2),
                    "oiPct": oi_pct, "buildup": calculate_oi_buildup(price_pct, oi_pct, item.get("is_fo", True), candle_tag),
                    "momentumScore": compute_aggressive_momentum(price_pct, vol_spike, candle_tag),
                    "candleTag": candle_tag, "is_fo": item.get("is_fo", True)
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

# ================= D&S ZONES LOGIC =================
def get_chart_benchmark(candles, idx, lookback=50):
    start = max(0, idx - lookback)
    subset = candles[start:idx]
    if not subset:
        c = candles[idx]
        return max(abs(c[4] - c[1]), 0.01), max(c[2] - c[3], 0.01)
    bodies = [abs(c[4] - c[1]) for c in subset]
    ranges = [max(c[2] - c[3], 0.0001) for c in subset]
    return max(sum(bodies) / len(bodies), 0.0001), max(sum(ranges) / len(ranges), 0.0001)

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

def scan_chart_for_active_zones(candles, current_ltp, tf="day"):
    if len(candles) < 12: return []
    raw_valid_zones = []
    start_idx = max(5, len(candles) - 150)
    for i in range(len(candles) - 1, start_idx, -1):
        c_out1 = candles[i]
        out1_o, out1_h, out1_l, out1_c = c_out1[1], c_out1[2], c_out1[3], c_out1[4]
        out1_range = out1_h - out1_l
        if out1_range <= 0: continue
        out1_body = abs(out1_c - out1_o)
        if (out1_body / out1_range) < 0.48: continue
        chart_avg_body, chart_avg_range = get_chart_benchmark(candles, max(1, i - 4), lookback=50)
        if out1_body < (chart_avg_body * 0.75): continue
        t_out1 = "RALLY" if out1_c > out1_o else "DROP"
        bias = "DEMAND" if t_out1 == "RALLY" else "SUPPLY"

        leg_outs = [c_out1]
        pattern = None
        proximal, distal = 0.0, 0.0
        ratio_vs_legin = 1.0

        for num_bases in (2, 1):
            if i - num_bases - 1 < 0: continue
            base_candles = candles[i - num_bases : i]
            c_in = candles[i - num_bases - 1]
            in_o, in_h, in_l, in_c = c_in[1], c_in[2], c_in[3], c_in[4]
            in_range = in_h - in_l
            if in_range <= 0: continue
            in_body = abs(in_c - in_o)
            if (in_body / in_range) < 0.48 or in_body < (chart_avg_body * 0.75): continue

            all_bases_valid = True
            for bc in base_candles:
                bc_o, bc_h, bc_l, bc_c = bc[1], bc[2], bc[3], bc[4]
                bc_rng = bc_h - bc_l
                bc_body = abs(bc_c - bc_o)
                if bc_rng <= 0 or (bc_body / bc_rng) > 0.55 or bc_body > (in_body * 0.55):
                    all_bases_valid = False
                    break
            if not all_bases_valid: continue

            if bias == "DEMAND":
                proximal = max(max(bc[1], bc[4]) for bc in base_candles)
                distal = min(bc[3] for bc in base_candles)
            else:
                proximal = min(min(bc[1], bc[4]) for bc in base_candles)
                distal = max(bc[2] for bc in base_candles)

            cluster_high = max(bc[2] for bc in base_candles)
            cluster_low = min(bc[3] for bc in base_candles)
            cluster_range = cluster_high - cluster_low
            if cluster_range <= 0 or cluster_range > (chart_avg_range * 1.45): continue

            in_prefix = "R" if in_c > in_o else "D"
            base_str = "-".join(["B"] * num_bases)
            out_str = "-".join(["R" if t_out1 == "RALLY" else "D"] * len(leg_outs))
            pattern = f"{in_prefix}-{base_str}-{out_str}"
            ratio_vs_legin = round(sum(abs(c[4]-c[1]) for c in leg_outs) / max(in_body, 0.01), 2)
            break

        if not pattern or proximal <= 0 or distal <= 0 or proximal == distal: continue
        risk = round(abs(proximal - distal), 2)
        if risk <= 0: continue
        if bias == "DEMAND" and current_ltp < distal: continue
        if bias == "SUPPLY" and current_ltp > distal: continue

        violated = False
        touch_count = 0
        has_left_zone = (i == len(candles) - 1)
        for j in range(i + 1, len(candles)):
            c_high, c_low = candles[j][2], candles[j][3]
            if bias == "DEMAND":
                if c_low < distal: violated = True; break
                if not has_left_zone:
                    if c_low > proximal: has_left_zone = True
                else:
                    if c_low <= proximal: touch_count += 1
            else:
                if c_high > distal: violated = True; break
                if not has_left_zone:
                    if c_high < proximal: has_left_zone = True
                else:
                    if c_high >= proximal: touch_count += 1

        if violated or not has_left_zone: continue
        status = "Untested (Fresh)" if touch_count == 0 else f"Tested ({touch_count}x)"
        if bias == "DEMAND":
            if distal <= current_ltp <= proximal: status = "🎯 In Zone (Actionable)"
            elif proximal < current_ltp <= proximal + (risk * 2.5): status = "⚡ Approaching H2"
        else:
            if proximal <= current_ltp <= distal: status = "🎯 In Zone (Actionable)"
            elif proximal - (risk * 2.5) <= current_ltp < proximal: status = "⚡ Approaching L2"

        dist_pct = (abs(proximal - current_ltp) / current_ltp) * 100 if current_ltp > 0 else 100.0
        raw_valid_zones.append({
            "pattern": pattern, "bias": bias, "proximal": round(proximal, 2), "distal": round(distal, 2),
            "risk": risk, "status": status, "multiplier": ratio_vs_legin, "dist_pct": dist_pct
        })

    distinct_zones = []
    for z in raw_valid_zones:
        z_low, z_high = min(z["proximal"], z["distal"]), max(z["proximal"], z["distal"])
        if not any(e["bias"] == z["bias"] and max(z_low, min(e["proximal"], e["distal"])) <= min(z_high, max(e["proximal"], e["distal"])) for e in distinct_zones):
            distinct_zones.append(z)
    distinct_zones.sort(key=lambda x: x["dist_pct"])
    for z in distinct_zones: del z["dist_pct"]
    return distinct_zones

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

# ================= BACKGROUND SCHEDULER FOR 9:20 AM SMART ORB =================
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

# ================= ENDPOINTS =================
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

@app.route("/")
def index():
    return send_from_directory('.', 'index.html')

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

        # Limit to top liquid stocks or process in smaller batches to avoid rate limits
        active_sample = target_universe[:75]  # Scans top 75 liquid instruments to prevent timeout

        def worker(item):
            try:
                candles = fetch_historical_candles(item["isin"], tf)
                if not candles or len(candles) < 10: 
                    return []
                ltp = live_quotes.get(item["symbol"], candles[-1][4] if candles else 0)
                zones = scan_chart_for_active_zones(candles, ltp, tf)
                return [{
                    "symbol": item["symbol"], "sector": item["sector"], "ltp": ltp,
                    "pattern": z["pattern"], "bias": z["bias"], "proximal": z["proximal"], "distal": z["distal"],
                    "risk": z["risk"], "risk_pct": round((z["risk"] / ltp) * 100, 2) if ltp > 0 else 0,
                    "status": z["status"], "multiplier": z.get("multiplier", 1.0)
                } for z in zones]
            except Exception:
                return []

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            for res in executor.map(worker, active_sample):
                if res: 
                    results.extend(res)

        results.sort(key=lambda x: (0 if x["pattern"].startswith("R-B-") and x["bias"] == "DEMAND" else 1, 0 if "🎯" in x["status"] else 1 if "⚡" in x["status"] else 2, -x.get("multiplier", 1.0)))
        
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
                    "price": f"{avg_pct:+.2f}%",
                    "change": "",
                    "isPositive": avg_pct >= 0
                })

        sorted_by_gain = sorted(stocks, key=lambda x: float(x.get("pricePct", 0.0)), reverse=True)
        if len(sorted_by_gain) > 0:
            top_gainer = sorted_by_gain[0]
            ticker_results.append({"symbol": f"TOP GAINER: {top_gainer['symbol']}", "price": f"₹{top_gainer['ltp']:,.2f}", "change": f"{top_gainer['pricePct']:+.2f}%", "isPositive": True})
            top_loser = sorted_by_gain[-1]
            ticker_results.append({"symbol": f"TOP LOSER: {top_loser['symbol']}", "price": f"₹{top_loser['ltp']:,.2f}", "change": f"{top_loser['pricePct']:+.2f}%", "isPositive": False})

    return jsonify({"status": "success", "data": ticker_results})

if __name__ == "__main__":
    start_smart_orb_background_thread()
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False, threaded=True)
