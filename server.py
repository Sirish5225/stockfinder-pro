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

app = Flask(__name__)
CORS(app)

# Persistent HTTP Session Pool to prevent Windows Socket [WinError 10038] crashes
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

ALL_NSE_EQ_MAP = {}

def load_all_upstox_instruments():
    global ALL_NSE_EQ_MAP
    if ALL_NSE_EQ_MAP:
        return ALL_NSE_EQ_MAP

    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=15) as response:
            with gzip.open(response, 'rt', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) < 4:
                        continue
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
            universe.append({
                "symbol": symbol,
                "name": inst_map[symbol]["name"],
                "sector": sector,
                "isin": inst_map[symbol]["inst_key"],
                "is_fo": True
            })
    if not universe:
        return [{"symbol": "NIFTY", "name": "Fallback", "sector": "Error", "isin": "NSE_INDEX|Nifty 50", "is_fo": False}]
    return universe

GLOBAL_MARKET_UNIVERSE = load_dynamic_universe()
CORE_MARKET_UNIVERSE = GLOBAL_MARKET_UNIVERSE.copy()
NIFTY500_UNIVERSE_CACHE = []

def get_nifty_500_data():
    urls = [
        'https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv',
        'https://archives.nseindia.com/content/indices/ind_nifty500list.csv'
    ]
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/csv,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Referer': 'https://www.nseindia.com/'
    }
    for url in urls:
        try:
            res = HTTP_SESSION.get(url, headers=headers, timeout=8)
            if res.status_code == 200 and "Symbol" in res.text:
                lines = res.text.strip().splitlines()
                reader = csv.reader(lines)
                next(reader, None)
                return {row[2].strip(): row[1].strip() for row in reader if len(row) > 2}
        except Exception as e:
            print(f"NSE Nifty 500 fetch error ({url}):", e)
    return {}

def load_nifty500_universe():
    global NIFTY500_UNIVERSE_CACHE
    if NIFTY500_UNIVERSE_CACHE:
        return NIFTY500_UNIVERSE_CACHE

    inst_map = load_all_upstox_instruments()
    nifty_data = get_nifty_500_data()
    universe = []
    seen = set()

    if nifty_data:
        for symbol, nse_sector in nifty_data.items():
            if symbol in inst_map and symbol not in seen:
                seen.add(symbol)
                is_fo_stock = symbol in TARGET_FO_SYMBOLS
                sector_name = TARGET_FO_SYMBOLS.get(symbol) or nse_sector or "NIFTY 500"
                universe.append({
                    "symbol": symbol,
                    "name": inst_map[symbol]["name"],
                    "sector": sector_name,
                    "isin": inst_map[symbol]["inst_key"],
                    "is_fo": is_fo_stock
                })
    else:
        for item in CORE_MARKET_UNIVERSE:
            seen.add(item["symbol"])
            universe.append(item.copy())
        for symbol, info in inst_map.items():
            if symbol not in seen:
                seen.add(symbol)
                universe.append({
                    "symbol": symbol,
                    "name": info["name"],
                    "sector": "Mid & Smallcap Cash",
                    "isin": info["inst_key"],
                    "is_fo": False
                })
                if len(universe) >= 500:
                    break

    if universe:
        NIFTY500_UNIVERSE_CACHE = universe
    return universe

def get_active_universe_list(univ_type=None):
    u = (univ_type or request.args.get("universe", "FO")).upper()
    if u == "NIFTY500":
        return "NIFTY500", load_nifty500_universe()
    return "FO", GLOBAL_MARKET_UNIVERSE

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
        if not chunk:
            continue
        o = chunk[0][1]
        h = max(c[2] for c in chunk)
        l = min(c[3] for c in chunk)
        c_close = chunk[-1][4]
        vol = sum(c[5] for c in chunk)
        aggregated.append([chunk[0][0], o, h, l, c_close, vol])
    return aggregated

def scan_chart_for_active_zones(candles, current_ltp, tf="day"):
    if len(candles) < 12:
        return []

    raw_valid_zones = []
    start_idx = max(5, len(candles) - 150)

    for i in range(len(candles) - 1, start_idx, -1):
        c_out1 = candles[i]
        out1_o, out1_h, out1_l, out1_c = c_out1[1], c_out1[2], c_out1[3], c_out1[4]
        out1_range = out1_h - out1_l
        if out1_range <= 0:
            continue
        out1_body = abs(out1_c - out1_o)
        out1_body_pct = out1_body / out1_range

        chart_avg_body, chart_avg_range = get_chart_benchmark(candles, max(1, i - 4), lookback=50)

        if out1_body_pct < 0.48 or out1_body < (chart_avg_body * 0.75):
            continue

        t_out1 = "RALLY" if out1_c > out1_o else "DROP"
        bias = "DEMAND" if t_out1 == "RALLY" else "SUPPLY"

        leg_outs = [c_out1]
        if i + 1 < len(candles):
            c_out2 = candles[i + 1]
            out2_o, out2_h, out2_l, out2_c = c_out2[1], c_out2[2], c_out2[3], c_out2[4]
            out2_range = out2_h - out2_l
            out2_body = abs(out2_c - out2_o)
            if out2_range > 0 and (out2_body / out2_range) >= 0.48 and out2_body >= (chart_avg_body * 0.75):
                t_out2 = "RALLY" if out2_c > out2_o else "DROP"
                if t_out2 == t_out1:
                    if (bias == "DEMAND" and out2_c > out1_c) or (bias == "SUPPLY" and out2_c < out1_c):
                        leg_outs.append(c_out2)

        out_str = "-".join(["R" if t_out1 == "RALLY" else "D"] * len(leg_outs))
        total_out_body = sum(abs(c[4] - c[1]) for c in leg_outs)
        pattern = None
        proximal, distal = 0.0, 0.0
        ratio_vs_legin = 1.0

        for num_bases in (2, 1):
            if i - num_bases - 1 < 0:
                continue

            base_candles = candles[i - num_bases : i]
            c_in = candles[i - num_bases - 1]

            in_o, in_h, in_l, in_c = c_in[1], c_in[2], c_in[3], c_in[4]
            in_range = in_h - in_l
            in_body = abs(in_c - in_o)
            if in_range <= 0:
                continue
            in_body_pct = in_body / in_range

            if in_body_pct < 0.48 or in_body < (chart_avg_body * 0.75):
                continue

            if num_bases == 1 and i - 3 >= 0:
                prev_c = candles[i - 3]
                prev_body = abs(prev_c[4] - prev_c[1])
                if in_body <= (chart_avg_body * 0.80) and prev_body >= (in_body * 1.40):
                    continue

            all_bases_valid = True
            for bc in base_candles:
                bc_o, bc_h, bc_l, bc_c = bc[1], bc[2], bc[3], bc[4]
                bc_rng = bc_h - bc_l
                bc_body = abs(bc_c - bc_o)
                if bc_rng <= 0:
                    all_bases_valid = False
                    break
                bc_body_pct = bc_body / bc_rng

                is_small_base_candle = (
                    bc_body <= (in_body * 0.55)
                    and bc_body <= (out1_body * 0.55)
                    and bc_body <= (chart_avg_body * 0.85)
                    and (bc_body_pct <= 0.55 or bc_body <= (in_body * 0.35))
                )
                if not is_small_base_candle:
                    all_bases_valid = False
                    break

            if not all_bases_valid:
                continue

            # Strict Proximal & Distal calculations
            if bias == "DEMAND":
                proximal = max(max(bc[1], bc[4]) for bc in base_candles)
                distal = min(bc[3] for bc in base_candles)
            else:
                proximal = min(min(bc[1], bc[4]) for bc in base_candles)
                distal = max(bc[2] for bc in base_candles)

            cluster_high = max(bc[2] for bc in base_candles)
            cluster_low = min(bc[3] for bc in base_candles)
            cluster_range = cluster_high - cluster_low
            if cluster_range <= 0:
                continue

            max_cluster_mult = 1.20 if num_bases == 1 else 1.45
            if cluster_range > (chart_avg_range * max_cluster_mult):
                continue

            in_prefix = "R" if in_c > in_o else "D"
            structure_ok = False

            if bias == "DEMAND":
                if in_prefix == "R":
                    base_holds_gains = (
                        cluster_low >= in_l + (in_range * 0.20)
                        and min( min(bc[1], bc[4]) for bc in base_candles ) >= in_o + (in_body * 0.30)
                    )
                    clean_breakout = out1_c > cluster_high and out1_c > in_h
                    if base_holds_gains and clean_breakout:
                        structure_ok = True
                else:
                    swing_win = candles[max(0, i - num_bases - 5) : i + 1]
                    if out1_c > cluster_high and min(in_l, cluster_low) <= min(c[3] for c in swing_win):
                        structure_ok = True
            else:
                if in_prefix == "D":
                    base_holds_drop = (
                        cluster_high <= in_h - (in_range * 0.20)
                        and max( max(bc[1], bc[4]) for bc in base_candles ) <= in_o - (in_body * 0.30)
                    )
                    clean_breakdown = out1_c < cluster_low and out1_c < in_l
                    if base_holds_drop and clean_breakdown:
                        structure_ok = True
                else:
                    swing_win = candles[max(0, i - num_bases - 5) : i + 1]
                    if out1_c < cluster_low and max(in_h, cluster_high) >= max(c[2] for c in swing_win):
                        structure_ok = True

            if structure_ok:
                base_str = "-".join(["B"] * num_bases)
                pattern = f"{in_prefix}-{base_str}-{out_str}"
                ratio_vs_legin = round(total_out_body / max(in_body, 0.01), 2)
                break

        if not pattern or proximal <= 0 or distal <= 0 or proximal == distal:
            continue

        risk = round(abs(proximal - distal), 2)
        if risk <= 0:
            continue

        if bias == "DEMAND" and current_ltp < distal:
            continue
        if bias == "SUPPLY" and current_ltp > distal:
            continue

        violated = False
        touch_count = 0
        has_left_zone = (i == len(candles) - 1)

        for j in range(i + 1, len(candles)):
            c_high, c_low = candles[j][2], candles[j][3]
            if bias == "DEMAND":
                if c_low < distal:
                    violated = True
                    break
                if not has_left_zone:
                    if c_low > proximal:
                        has_left_zone = True
                else:
                    if c_low <= proximal:
                        touch_count += 1
            else:
                if c_high > distal:
                    violated = True
                    break
                if not has_left_zone:
                    if c_high < proximal:
                        has_left_zone = True
                else:
                    if c_high >= proximal:
                        touch_count += 1

        if violated or not has_left_zone:
            continue

        status = "Untested (Fresh)" if touch_count == 0 else f"Tested ({touch_count}x)"
        if bias == "DEMAND":
            if distal <= current_ltp <= proximal:
                status = "🎯 In Zone (Actionable)"
            elif proximal < current_ltp <= proximal + (risk * 2.5):
                status = "⚡ Approaching H2"
        else:
            if proximal <= current_ltp <= distal:
                status = "🎯 In Zone (Actionable)"
            elif proximal - (risk * 2.5) <= current_ltp < proximal:
                status = "⚡ Approaching L2"

        dist_pct = (abs(proximal - current_ltp) / current_ltp) * 100 if current_ltp > 0 else 100.0
        is_rbr_demand = pattern.startswith("R-B-") and bias == "DEMAND"
        setup_rank = 0 if is_rbr_demand else 1

        priority_rank = (
            0 if "🎯" in status else
            1 if "⚡" in status else
            2 if "Untested" in status else 3
        )

        raw_valid_zones.append({
            "pattern": pattern, "bias": bias,
            "proximal": round(proximal, 2), "distal": round(distal, 2),
            "risk": risk, "status": status, "multiplier": ratio_vs_legin,
            "priority_rank": priority_rank, "setup_rank": setup_rank, "dist_pct": dist_pct
        })

    if not raw_valid_zones:
        return []

    distinct_zones = []
    for z in raw_valid_zones:
        z_low, z_high = min(z["proximal"], z["distal"]), max(z["proximal"], z["distal"])
        is_duplicate = False
        for existing in distinct_zones:
            if existing["bias"] == z["bias"]:
                e_low, e_high = min(existing["proximal"], existing["distal"]), max(existing["proximal"], existing["distal"])
                if max(z_low, e_low) <= min(z_high, e_high):
                    is_duplicate = True
                    break
        if not is_duplicate:
            distinct_zones.append(z)

    distinct_zones.sort(key=lambda x: (x["setup_rank"], x["priority_rank"], x["dist_pct"]))
    for z in distinct_zones:
        del z["priority_rank"]
        del z["setup_rank"]
        del z["dist_pct"]

    return distinct_zones

def fetch_historical_candles(instrument_key, interval):
    global CANDLE_CACHE
    api_interval = "month" if interval in ["quarter", "halfyear"] else interval
    cache_key = f"{instrument_key}_{interval}"

    cached = CANDLE_CACHE.get(cache_key)
    if cached and (time.time() - cached["ts"] < 900) and cached["candles"]:
        return cached["candles"]

    to_date = date.today()
    if interval in ["1minute", "5minute", "15minute", "30minute", "60minute"]:
        from_date = to_date - timedelta(days=10 if interval == "5minute" else 30)
    else:
        from_date = to_date - timedelta(days=730 if interval == "day" else 1825)

    to_date_str = to_date.strftime('%Y-%m-%d')
    from_date_str = from_date.strftime('%Y-%m-%d')

    encoded_key = urllib.parse.quote(instrument_key, safe='')
    url = f"https://api.upstox.com/v2/historical-candle/{encoded_key}/{api_interval}/{to_date_str}/{from_date_str}"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}

    for attempt in range(3):
        try:
            res = HTTP_SESSION.get(url, headers=headers, timeout=6)
            if res.status_code == 200:
                candles = res.json().get("data", {}).get("candles", [])
                candles.reverse()
                if interval == "quarter":
                    candles = aggregate_candles(candles, 3)
                elif interval == "halfyear":
                    candles = aggregate_candles(candles, 6)
                if candles:
                    CANDLE_CACHE[cache_key] = {"candles": candles, "ts": time.time()}
                return candles
            elif res.status_code == 429:
                time.sleep(0.3 * (attempt + 1))
        except Exception:
            time.sleep(0.2)
    return []

@app.route("/api/zone-screener", methods=["GET"])
def zone_screener():
    global ZONE_CACHE
    tf = request.args.get("tf", "day").lower()
    force_refresh = request.args.get("t") is not None
    univ_key, target_universe = get_active_universe_list()

    valid_tfs = ["5minute", "15minute", "30minute", "60minute", "day", "week", "month", "quarter", "halfyear"]
    if tf not in valid_tfs:
        tf = "day"

    univ_zone_cache = ZONE_CACHE.setdefault(univ_key, {"last_updated": {}})
    if not force_refresh and (time.time() - univ_zone_cache["last_updated"].get(tf, 0) < 45) and len(univ_zone_cache.get(tf, [])) > 0:
        return jsonify({"status": "success", "data": univ_zone_cache[tf]})

    quotes_res = fetch_live_upstox_quotes(univ_key)
    live_quotes = {s["symbol"]: s["ltp"] for s in (quotes_res if isinstance(quotes_res, list) else [])}
    results = []

    def worker(item):
        candles = fetch_historical_candles(item["isin"], tf)
        if not candles:
            return []
        ltp = live_quotes.get(item["symbol"], candles[-1][4] if candles else 0)
        zones = scan_chart_for_active_zones(candles, ltp, tf)
        stock_zones = []
        for zone in zones:
            risk_pct = round((zone["risk"] / ltp) * 100, 2) if ltp > 0 else 0
            stock_zones.append({
                "symbol": item["symbol"], "sector": item["sector"], "ltp": ltp,
                "pattern": zone["pattern"], "bias": zone["bias"],
                "proximal": zone["proximal"], "distal": zone["distal"],
                "risk": zone["risk"], "risk_pct": risk_pct, "status": zone["status"],
                "multiplier": zone.get("multiplier", 1.0)
            })
        return stock_zones

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(worker, stock) for stock in target_universe]
        for f in concurrent.futures.as_completed(futures):
            res = f.result()
            if res:
                results.extend(res)

    results.sort(key=lambda x: (
        0 if x["pattern"].startswith("R-B-") and x["bias"] == "DEMAND" else 1,
        0 if "🎯" in x["status"] else 1 if "⚡" in x["status"] else 2 if "Untested" in x["status"] else 3,
        -x.get("multiplier", 1.0)
    ))

    univ_zone_cache[tf] = results
    univ_zone_cache["last_updated"][tf] = time.time()

    return jsonify({"status": "success", "data": results})

def detect_open_setup(open_p, high_p, low_p):
    if open_p <= 0 or low_p <= 0 or high_p <= 0:
        return None
    if abs(open_p - low_p) <= 0.05 or abs(open_p - low_p) <= (open_p * 0.0005):
        return "OPEN=LOW"
    elif abs(open_p - high_p) <= 0.05 or abs(open_p - high_p) <= (open_p * 0.0005):
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
    base_score = abs(price_pct) * 15.0
    spike_bonus = vol_spike * 10.0
    score = base_score + spike_bonus
    if candle_tag in ["OPEN=LOW", "OPEN=HIGH"]:
        score += 35.0
    return round(score, 1)

def fetch_live_upstox_quotes(univ_type="FO"):
    global QUOTE_CACHE
    if not UPSTOX_ACCESS_TOKEN or UPSTOX_ACCESS_TOKEN == "YOUR_NEW_TOKEN_HERE":
        return {"error": "Invalid API Token."}

    univ_key, target_universe = get_active_universe_list(univ_type)
    cache_entry = QUOTE_CACHE.setdefault(univ_key, {"data": [], "last_updated": 0})

    if time.time() - cache_entry["last_updated"] < 5 and cache_entry["data"]:
        return cache_entry["data"]

    results = []
    chunk_size = 100
    headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}

    def fetch_chunk(chunk):
        chunk_results = []
        keys_param = ",".join([item["isin"] for item in chunk])
        url = "https://api.upstox.com/v2/market-quote/quotes"
        try:
            res = HTTP_SESSION.get(url, headers=headers, params={"instrument_key": keys_param}, timeout=8)
            if res.status_code == 401:
                return {"error": "Upstox API Token Expired."}
            elif res.status_code != 200:
                return []
            quotes_data = res.json().get("data", {})

            for item in chunk:
                sym = item["symbol"]
                isin_raw = item["isin"]
                isin_colon = isin_raw.replace("|", ":")

                q = quotes_data.get(isin_raw) or quotes_data.get(isin_colon) or quotes_data.get(f"NSE_EQ:{sym}") or quotes_data.get(sym)
                if not q:
                    continue

                ltp = float(q.get("last_price", 0.0))
                ohlc = q.get("ohlc", {})
                open_p = float(ohlc.get("open") or 0.0)
                low_p = float(ohlc.get("low") or 0.0)
                high_p = float(ohlc.get("high") or 0.0)
                volume = int(q.get("volume", 0))

                average_volume = int(q.get("average_volume") or q.get("avg_volume") or (volume * 0.6) or 100000)
                net_change = float(q.get("net_change") or 0.0)
                prev_close = float(q.get("prev_close") or q.get("prev_close_price") or 0.0)
                if prev_close == 0.0 and (ltp - net_change) > 0:
                    prev_close = ltp - net_change

                price_pct = round(((ltp - prev_close) / prev_close) * 100, 2) if prev_close > 0 else 0.0
                candle_tag = detect_open_setup(open_p, high_p, low_p)
                oi = float(q.get("oi") or 0.0)
                prev_oi = float(q.get("prev_oi") or 0.0)
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
            if isinstance(res, dict) and "error" in res:
                return res
            if isinstance(res, list):
                results.extend(res)

    if results:
        results.sort(key=lambda x: x["momentumScore"], reverse=True)
        cache_entry["data"] = results
        cache_entry["last_updated"] = time.time()
        return results
    return {"error": "No market data retrieved."}

@app.route("/")
def index():
    return send_from_directory('.', 'index.html')

@app.route("/api/set-universe", methods=["POST"])
def set_universe():
    data = request.json or {}
    univ_type = data.get("type", "FO").upper()
    univ_key, target_list = get_active_universe_list(univ_type)
    QUOTE_CACHE[univ_key] = {"data": [], "last_updated": 0}
    return jsonify({"status": "success", "message": f"Switched to {univ_key} Universe", "count": len(target_list)})

@app.route("/api/upload-universe", methods=["POST"])
def upload_universe():
    global GLOBAL_MARKET_UNIVERSE
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "No file uploaded"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"status": "error", "message": "Empty file name"}), 400

    try:
        stream = io.TextIOWrapper(file.stream, encoding="utf-8-sig")
        sample_line = stream.readline()
        stream.seek(0)
        inst_map = load_all_upstox_instruments()
        existing_isin_map = {item["symbol"]: item for item in CORE_MARKET_UNIVERSE}
        new_universe = []

        if ',' not in sample_line:
            reader = csv.reader(stream)
            for row in reader:
                if not row or not row[0].strip() or row[0].strip().upper() in ["SYMBOL", "TICKER", "STOCK"]:
                    continue
                sym = row[0].strip().upper()
                if sym in existing_isin_map:
                    new_universe.append(existing_isin_map[sym])
                elif sym in inst_map:
                    new_universe.append({"symbol": sym, "name": inst_map[sym]["name"], "sector": "Custom Universe", "isin": inst_map[sym]["inst_key"], "is_fo": False})
        else:
            reader = csv.DictReader(stream)
            for row in reader:
                cleaned_row = {k.strip().lower(): v.strip() for k, v in row.items() if k is not None}
                sym = cleaned_row.get("symbol") or cleaned_row.get("ticker") or cleaned_row.get("stock")
                if not sym:
                    continue
                sym = sym.upper()
                if sym in existing_isin_map:
                    item = existing_isin_map[sym].copy()
                    if cleaned_row.get("sector"):
                        item["sector"] = cleaned_row.get("sector")
                    new_universe.append(item)
                elif sym in inst_map:
                    new_universe.append({"symbol": sym, "name": cleaned_row.get("name") or inst_map[sym]["name"], "sector": cleaned_row.get("sector") or "Custom Universe", "isin": inst_map[sym]["inst_key"], "is_fo": False})

        if new_universe:
            GLOBAL_MARKET_UNIVERSE = new_universe
            QUOTE_CACHE["FO"] = {"data": [], "last_updated": 0}
            return jsonify({"status": "success", "message": f"Successfully loaded {len(new_universe)} stocks into screener universe!"})
        else:
            return jsonify({"status": "error", "message": "No valid rows found in the uploaded file."}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/reset-universe", methods=["POST"])
def reset_universe():
    global GLOBAL_MARKET_UNIVERSE
    GLOBAL_MARKET_UNIVERSE = CORE_MARKET_UNIVERSE.copy()
    QUOTE_CACHE["FO"] = {"data": [], "last_updated": 0}
    return jsonify({"status": "success", "message": f"Successfully reset to default universe ({len(GLOBAL_MARKET_UNIVERSE)} stocks)!"})

@app.route("/api/screener", methods=["GET"])
def screener():
    filter_val = request.args.get("filter", "ALL")
    search_val = request.args.get("search", "").strip().lower()
    univ_type = request.args.get("universe", "FO").upper()

    stocks = fetch_live_upstox_quotes(univ_type)
    if isinstance(stocks, dict) and "error" in stocks:
        return jsonify({"status": "error", "message": stocks["error"]})

    def get_price_pct(item):
        return float(item.get("pricePct", 0.0))

    top_gainers = [s for s in sorted(stocks, key=get_price_pct, reverse=True) if float(s.get("pricePct", 0.0)) > 0][:20]
    top_losers = [s for s in sorted(stocks, key=get_price_pct) if float(s.get("pricePct", 0.0)) < 0][:20]

    filtered_stocks = list(stocks)
    if filter_val and filter_val != "ALL":
        if filter_val in ["OPEN=LOW", "OPEN=HIGH"]:
            filtered_stocks = [x for x in filtered_stocks if x.get("candleTag") == filter_val]
        elif filter_val == "VOL_SHOCKER":
            filtered_stocks = [x for x in filtered_stocks if x.get("volMultiplier", 0) >= 1.5]
        else:
            filtered_stocks = [x for x in filtered_stocks if x.get("buildup", "").lower() == filter_val.lower()]

    if search_val:
        filtered_stocks = [x for x in filtered_stocks if search_val in x["symbol"].lower() or search_val in x["name"].lower()]

    return jsonify({
        "status": "success",
        "universe": univ_type,
        "timestamp": datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%H:%M:%S"),
        "count": len(filtered_stocks),
        "data": filtered_stocks,
        "topGainers": top_gainers,
        "topLosers": top_losers
    })

@app.route("/api/sectors", methods=["GET"])
def get_sectors():
    univ_type = request.args.get("universe", "FO").upper()
    stocks = fetch_live_upstox_quotes(univ_type)
    if isinstance(stocks, dict) and "error" in stocks:
        return jsonify({"status": "error", "message": stocks["error"]})

    target_stocks = [s for s in stocks if s.get("is_fo", True)] if univ_type == "FO" else stocks

    sec_map = defaultdict(lambda: {"stocks": 0, "total_pct": 0.0, "total_vol": 0, "advances": 0, "declines": 0})
    total_market_vol = total_market_advances = total_market_declines = 0

    for s in target_stocks:
        sec = s.get("sector", "Market Universe")
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
        "status": "success",
        "universe": univ_type,
        "data": summary,
        "marketBreadth": {"totalVolume": total_market_vol, "advances": total_market_advances, "declines": total_market_declines}
    })

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
        def get_price_pct_item(item):
            return float(item.get("pricePct", 0.0))
        sorted_by_gain = sorted(stocks, key=get_price_pct_item, reverse=True)
        if len(sorted_by_gain) > 0:
            top_gainer = sorted_by_gain[0]
            ticker_results.append({"symbol": f"TOP GAINER: {top_gainer['symbol']}", "price": f"{top_gainer['ltp']:,.2f}", "change": f"{top_gainer['pricePct']:+.2f}%", "isPositive": True})
            top_loser = sorted_by_gain[-1]
            ticker_results.append({"symbol": f"TOP LOSER: {top_loser['symbol']}", "price": f"{top_loser['ltp']:,.2f}", "change": f"{top_loser['pricePct']:+.2f}%", "isPositive": False})

    return jsonify({"status": "success", "data": ticker_results})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False, threaded=True)
