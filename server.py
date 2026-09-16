import os
import requests
from datetime import datetime
import pytz
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from collections import defaultdict

# ఇక్కడ static_folder తొలగించబడింది, కాబట్టి ఇది నేరుగా రూట్ ఫైల్స్‌ని తీసుకుంటుంది
app = Flask(__name__)
CORS(app)

UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiJDTDgwMDQiLCJqdGkiOiI2YTZlZGU0YTdkMDdkYzI0NTcxY2IwNjgiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlzRXh0ZW5kZWQiOnRydWUsImlhdCI6MTc4NTY1MDc2MiwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxODE3MjQ0MDAwfQ.QevX5BwRdiDzZNmuSc0CGqDZcN5VP1qK6GXbvziEAik").strip()

CORE_MARKET_UNIVERSE = [
    {"symbol": "HDFCBANK", "name": "HDFC Bank Ltd", "sector": "Nifty Bank", "isin": "NSE_EQ|INE040A01034", "is_fo": True},
    {"symbol": "ICICIBANK", "name": "ICICI Bank Ltd", "sector": "Nifty Bank", "isin": "NSE_EQ|INE090A01021", "is_fo": True},
    {"symbol": "SBIN", "name": "State Bank of India", "sector": "Nifty Bank", "isin": "NSE_EQ|INE062A01020", "is_fo": True},
    {"symbol": "AXISBANK", "name": "Axis Bank Limited", "sector": "Nifty Bank", "isin": "NSE_EQ|INE238A01034", "is_fo": True},
    {"symbol": "KOTAKBANK", "name": "Kotak Mahindra Bank", "sector": "Nifty Bank", "isin": "NSE_EQ|INE237A01028", "is_fo": True},
    {"symbol": "INDUSINDBK", "name": "IndusInd Bank Limited", "sector": "Nifty Bank", "isin": "NSE_EQ|INE095A01012", "is_fo": True},
    {"symbol": "BANKBARODA", "name": "Bank of Baroda", "sector": "Nifty Bank", "isin": "NSE_EQ|INE077A01010", "is_fo": True},
    {"symbol": "PNB", "name": "Punjab National Bank", "sector": "Nifty Bank", "isin": "NSE_EQ|INE160A01022", "is_fo": True},
    {"symbol": "AUBANK", "name": "AU Small Finance Bank Ltd", "sector": "Nifty Bank", "isin": "NSE_EQ|INE949L01017", "is_fo": True},
    {"symbol": "FEDERALBNK", "name": "The Federal Bank Ltd", "sector": "Nifty Bank", "isin": "NSE_EQ|INE171A01029", "is_fo": True},
    {"symbol": "IDFCFIRSTB", "name": "IDFC First Bank Limited", "sector": "Nifty Bank", "isin": "NSE_EQ|INE092T01019", "is_fo": True},
    {"symbol": "BANDHANBNK", "name": "Bandhan Bank Limited", "sector": "Nifty Bank", "isin": "NSE_EQ|INE545U01014", "is_fo": True},
    {"symbol": "BAJFINANCE", "name": "Bajaj Finance Limited", "sector": "Nifty Fin Service", "isin": "NSE_EQ|INE296A01024", "is_fo": True},
    {"symbol": "BAJAJFINSV", "name": "Bajaj Finserv Limited", "sector": "Nifty Fin Service", "isin": "NSE_EQ|INE918I01018", "is_fo": True},
    {"symbol": "TCS", "name": "Tata Consultancy Services", "sector": "Nifty It", "isin": "NSE_EQ|INE467B01029", "is_fo": True},
    {"symbol": "INFY", "name": "Infosys Limited", "sector": "Nifty It", "isin": "NSE_EQ|INE009A01021", "is_fo": True},
    {"symbol": "RELIANCE", "name": "Reliance Industries Ltd", "sector": "Nifty Energy", "isin": "NSE_EQ|INE002A01018", "is_fo": True},
    {"symbol": "TATASTEEL", "name": "Tata Steel Ltd", "sector": "Nifty Metal", "isin": "NSE_EQ|INE081A01020", "is_fo": True}
]

def calculate_oi_buildup(price_pct, oi_pct, is_fo):
    if not is_fo:
        return "Cash Equity"
    if price_pct > 0 and oi_pct > 0:
        return "Long Buildup"
    elif price_pct < 0 and oi_pct > 0:
        return "Short Buildup"
    elif price_pct > 0 and oi_pct < 0:
        return "Short Covering"
    elif price_pct < 0 and oi_pct < 0:
        return "Long Unwinding"
    return "Neutral"

def detect_open_setup(open_p, high_p, low_p):
    if open_p <= 0 or low_p <= 0 or high_p <= 0:
        return None
    if open_p == low_p:
        return "OPEN=LOW"
    elif open_p == high_p:
        return "OPEN=HIGH"
    return None

def compute_aggressive_momentum(price_pct, volume, average_volume, candle_tag):
    vol_spike = float(volume) / float(average_volume) if average_volume > 0 else 1.0
    base_score = abs(price_pct) * 15.0
    spike_bonus = vol_spike * 10.0
    score = base_score + spike_bonus
    if candle_tag in ["OPEN=LOW", "OPEN=HIGH"]:
        score += 35.0
    return round(score, 1)

def fetch_live_upstox_quotes():
    if not UPSTOX_ACCESS_TOKEN:
        return []
    results = []
    chunk_size = 40 
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"
    }
    for i in range(0, len(CORE_MARKET_UNIVERSE), chunk_size):
        chunk = CORE_MARKET_UNIVERSE[i:i + chunk_size]
        isin_keys = [item["isin"] for item in chunk]
        keys_param = ",".join(isin_keys)
        url = f"https://api.upstox.com/v2/market-quote/quotes?instrument_key={keys_param}"
        try:
            res = requests.get(url, headers=headers, timeout=10)
            res_json = res.json()
            if res.status_code != 200 or res_json.get("status") != "success":
                continue
            quotes_data = res_json.get("data", {})
            for item in chunk:
                sym = item["symbol"]
                isin_raw = item["isin"]
                q = quotes_data.get(isin_raw) or quotes_data.get(sym)
                if not q:
                    continue
                ltp = float(q.get("last_price", 0.0))
                ohlc = q.get("ohlc", {})
                open_p = float(ohlc.get("open") or 0.0)
                low_p = float(ohlc.get("low") or 0.0)
                high_p = float(ohlc.get("high") or 0.0)
                volume = int(q.get("volume", 0))
                average_volume = int(q.get("average_volume") or (volume * 0.6) or 100000)
                net_change = float(q.get("net_change") or 0.0)
                prev_close = float(q.get("prev_close") or (ltp - net_change))
                price_pct = round(((ltp - prev_close) / prev_close) * 100, 2) if prev_close > 0 else 0.0
                candle_tag = detect_open_setup(open_p, high_p, low_p)
                oi_pct = 1.2 if price_pct >= 0 else -1.2
                buildup = calculate_oi_buildup(price_pct, oi_pct, True)
                momentum_score = compute_aggressive_momentum(price_pct, volume, average_volume, candle_tag)
                vol_spike = round(float(volume) / float(average_volume), 1) if average_volume > 0 else 1.0

                results.append({
                    "symbol": sym,
                    "name": item["name"],
                    "sector": item.get("sector", "Others"),
                    "ltp": ltp,
                    "pricePct": price_pct,
                    "volume": volume,
                    "volSpike": vol_spike,
                    "oiPct": oi_pct,
                    "buildup": buildup,
                    "momentumScore": momentum_score,
                    "candleTag": candle_tag,
                    "is_fo": True
                })
        except Exception as e:
            print(f"[FETCH EXCEPTION] {e}")
    results.sort(key=lambda x: x["momentumScore"], reverse=True)
    return results

# --- నేరుగా మెయిన్ రూట్ నుండి index.html ని లోడ్ చేస్తుంది ---
@app.route("/")
def index():
    return send_from_directory('.', 'index.html')

@app.route("/api/screener", methods=["GET"])
def screener():
    filter_val = request.args.get("filter", "ALL")
    search_val = request.args.get("search", "").strip().lower()
    stocks = fetch_live_upstox_quotes()

    if filter_val and filter_val != "ALL":
        if filter_val == "OPEN=LOW":
            stocks = [x for x in stocks if x.get("candleTag") == "OPEN=LOW"]
        elif filter_val == "OPEN=HIGH":
            stocks = [x for x in stocks if x.get("candleTag") == "OPEN=HIGH"]
        else:
            stocks = [x for x in stocks if x.get("buildup", "").lower() == filter_val.lower()]

    if search_val:
        stocks = [x for x in stocks if search_val in x["symbol"].lower() or search_val in x["name"].lower()]

    ist = pytz.timezone("Asia/Kolkata")
    return jsonify({
        "status": "success",
        "timestamp": datetime.now(ist).strftime("%H:%M:%S"),
        "count": len(stocks),
        "data": stocks
    })

@app.route("/api/sectors", methods=["GET"])
def get_sectors():
    stocks = fetch_live_upstox_quotes()
    sec_map = defaultdict(lambda: {"stocks": 0, "total_pct": 0.0, "total_vol": 0, "advances": 0, "declines": 0})
    for s in stocks:
        sec = s.get("sector", "Others")
        p_pct = float(s.get("pricePct", 0.0))
        sec_map[sec]["stocks"] += 1
        sec_map[sec]["total_pct"] += p_pct
        sec_map[sec]["total_vol"] += int(s.get("volume", 0))
        if p_pct >= 0:
            sec_map[sec]["advances"] += 1
        else:
            sec_map[sec]["declines"] += 1

    summary = []
    for sec, val in sec_map.items():
        avg_pct = round(val["total_pct"] / val["stocks"], 2) if val["stocks"] > 0 else 0.0
        tot_stocks = val["stocks"]
        adv_pct = round((val["advances"] / tot_stocks) * 100, 1) if tot_stocks > 0 else 0
        dec_pct = round((val["declines"] / tot_stocks) * 100, 1) if tot_stocks > 0 else 0
        summary.append({
            "sector": sec,
            "avgChange": avg_pct,
            "stocksCount": tot_stocks,
            "advances": val["advances"],
            "declines": val["declines"],
            "advPct": adv_pct,
            "decPct": dec_pct,
            "totalVolume": val["total_vol"]
        })
    summary.sort(key=lambda x: x["avgChange"], reverse=True)
    return jsonify({"status": "success", "data": summary})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
