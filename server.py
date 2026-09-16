import os
import requests
from datetime import datetime
import pytz
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from collections import defaultdict

app = Flask(__name__, static_folder="public")
CORS(app)

UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiJDTDgwMDQiLCJqdGkiOiI2YTZlZGU0YTdkMDdkYzI0NTcxY2IwNjgiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlzRXh0ZW5kZWQiOnRydWUsImlhdCI6MTc4NTY1MDc2MiwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxODE3MjQ0MDAwfQ.QevX5BwRdiDzZNmuSc0CGqDZcN5VP1qK6GXbvziEAik").strip()

# అప్‌టాక్స్ స్టాండర్డ్ పైప్ ఫార్మాట్ తో కూడిన మార్కెట్ యూనివర్స్
CORE_MARKET_UNIVERSE = [
    # --- NIFTY BANK ---
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

    # --- NIFTY FIN SERVICE & OTHERS ---
    {"symbol": "BAJFINANCE", "name": "Bajaj Finance Limited", "sector": "Nifty Fin Service", "isin": "NSE_EQ|INE296A01024", "is_fo": True},
    {"symbol": "BAJAJFINSV", "name": "Bajaj Finserv Limited", "sector": "Nifty Fin Service", "isin": "NSE_EQ|INE918I01018", "is_fo": True},
    {"symbol": "CHOLAFIN", "name": "Cholamandalam Investment", "sector": "Nifty Fin Service", "isin": "NSE_EQ|INE121A01024", "is_fo": True},
    {"symbol": "SHRIRAMFIN", "name": "Shriram Finance Limited", "sector": "Nifty Fin Service", "isin": "NSE_EQ|INE721A01013", "is_fo": True},
    {"symbol": "MUTHOOTFIN", "name": "Muthoot Finance Limited", "sector": "Nifty Fin Service", "isin": "NSE_EQ|INE414G01012", "is_fo": True},
    {"symbol": "SBILIFE", "name": "SBI Life Insurance Company", "sector": "Nifty Fin Service", "isin": "NSE_EQ|INE123W01016", "is_fo": True},
    {"symbol": "HDFCLIFE", "name": "HDFC Life Insurance Co Ltd", "sector": "Nifty Fin Service", "isin": "NSE_EQ|INE795G01014", "is_fo": True},
    {"symbol": "ICICIGI", "name": "ICICI Lombard General Insurance", "sector": "Nifty Fin Service", "isin": "NSE_EQ|INE765G01014", "is_fo": True},
    {"symbol": "ICICIPRULI", "name": "ICICI Pru Life Insurance", "sector": "Nifty Fin Service", "isin": "NSE_EQ|INE726G01019", "is_fo": True},
    {"symbol": "SBICARD", "name": "SBI Cards and Payment Services", "sector": "Nifty Fin Service", "isin": "NSE_EQ|INE418E01026", "is_fo": True},
    {"symbol": "PFC", "name": "Power Finance Corporation", "sector": "Nifty Fin Service", "isin": "NSE_EQ|INE134E01011", "is_fo": True},
    {"symbol": "RECLTD", "name": "REC Limited", "sector": "Nifty Fin Service", "isin": "NSE_EQ|INE036B01020", "is_fo": True},
    {"symbol": "HDFCAMC", "name": "HDFC Asset Management Co", "sector": "Nifty Fin Service", "isin": "NSE_EQ|INE127G01017", "is_fo": True},
    {"symbol": "NAM-INDIA", "name": "Nippon Life India Asset Mgmt", "sector": "Nifty Fin Service", "isin": "NSE_EQ|INE298J01013", "is_fo": True},
    {"symbol": "POLICYBZR", "name": "PB Fintech Limited (Policybazaar)", "sector": "Nifty Fin Service", "isin": "NSE_EQ|INE417T01026", "is_fo": True},
    {"symbol": "MFSL", "name": "Max Financial Services Ltd", "sector": "Nifty Fin Service", "isin": "NSE_EQ|INE180A01020", "is_fo": True},
    {"symbol": "PAYTM", "name": "One97 Communications Ltd", "sector": "Nifty Fin Service", "isin": "NSE_EQ|INE982J01020", "is_fo": True},

    # --- NIFTY IT ---
    {"symbol": "TCS", "name": "Tata Consultancy Services", "sector": "Nifty It", "isin": "NSE_EQ|INE467B01029", "is_fo": True},
    {"symbol": "INFY", "name": "Infosys Limited", "sector": "Nifty It", "isin": "NSE_EQ|INE009A01021", "is_fo": True},
    {"symbol": "HCLTECH", "name": "HCL Technologies Limited", "sector": "Nifty It", "isin": "NSE_EQ|INE860A01027", "is_fo": True},
    {"symbol": "WIPRO", "name": "Wipro Limited", "sector": "Nifty It", "isin": "NSE_EQ|INE075A01022", "is_fo": True},
    {"symbol": "TECHM", "name": "Tech Mahindra Limited", "sector": "Nifty It", "isin": "NSE_EQ|INE669C01036", "is_fo": True},
    {"symbol": "LTIM", "name": "LTIMindtree Limited", "sector": "Nifty It", "isin": "NSE_EQ|INE214T01019", "is_fo": True},
    {"symbol": "MPHASIS", "name": "Mphasis Limited", "sector": "Nifty It", "isin": "NSE_EQ|INE356A01018", "is_fo": True},

    # --- NIFTY AUTO ---
    {"symbol": "TATAMOTORS", "name": "Tata Motors Ltd", "sector": "Nifty Auto", "isin": "NSE_EQ|INE155A01022", "is_fo": True},
    {"symbol": "MARUTI", "name": "Maruti Suzuki India", "sector": "Nifty Auto", "isin": "NSE_EQ|INE585B01010", "is_fo": True},
    {"symbol": "M&M", "name": "Mahindra & Mahindra Ltd", "sector": "Nifty Auto", "isin": "NSE_EQ|INE101A01026", "is_fo": True},
    {"symbol": "BAJAJ-AUTO", "name": "Bajaj Auto Limited", "sector": "Nifty Auto", "isin": "NSE_EQ|INE917I01010", "is_fo": True},
    {"symbol": "EICHERMOT", "name": "Eicher Motors Ltd", "sector": "Nifty Auto", "isin": "NSE_EQ|INE066A01021", "is_fo": True},
    {"symbol": "HEROMOTOCO", "name": "Hero MotoCorp Ltd", "sector": "Nifty Auto", "isin": "NSE_EQ|INE158A01026", "is_fo": True},

    # --- NIFTY PHARMA & HEALTHCARE ---
    {"symbol": "SUNPHARMA", "name": "Sun Pharma Industries", "sector": "Nifty Pharma", "isin": "NSE_EQ|INE044A01036", "is_fo": True},
    {"symbol": "CIPLA", "name": "Cipla Limited", "sector": "Nifty Pharma", "isin": "NSE_EQ|INE059A01026", "is_fo": True},
    {"symbol": "DRREDDY", "name": "Dr. Reddy's Laboratories", "sector": "Nifty Pharma", "isin": "NSE_EQ|INE089A01023", "is_fo": True},
    {"symbol": "DIVISLAB", "name": "Divi's Laboratories", "sector": "Nifty Pharma", "isin": "NSE_EQ|INE361B01024", "is_fo": True},
    {"symbol": "LUPIN", "name": "Lupin Limited", "sector": "Nifty Pharma", "isin": "NSE_EQ|INE326A01037", "is_fo": True},
    {"symbol": "AUROPHARMA", "name": "Aurobindo Pharma", "sector": "Nifty Pharma", "isin": "NSE_EQ|INE406A01037", "is_fo": True},
    {"symbol": "APOLLOHOSP", "name": "Apollo Hospitals Enterprise", "sector": "Nifty Healthcare", "isin": "NSE_EQ|INE437A01024", "is_fo": True},

    # --- NIFTY METAL ---
    {"symbol": "TATASTEEL", "name": "Tata Steel Ltd", "sector": "Nifty Metal", "isin": "NSE_EQ|INE081A01020", "is_fo": True},
    {"symbol": "JINDALSTEL", "name": "Jindal Steel & Power", "sector": "Nifty Metal", "isin": "NSE_EQ|INE220G01021", "is_fo": True},
    {"symbol": "JSWSTEEL", "name": "JSW Steel Limited", "sector": "Nifty Metal", "isin": "NSE_EQ|INE019A01038", "is_fo": True},
    {"symbol": "HINDALCO", "name": "Hindalco Industries", "sector": "Nifty Metal", "isin": "NSE_EQ|INE038A01020", "is_fo": True},
    {"symbol": "VEDL", "name": "Vedanta Limited", "sector": "Nifty Metal", "isin": "NSE_EQ|INE205A01025", "is_fo": True},
    {"symbol": "NMDC", "name": "NMDC Limited", "sector": "Nifty Metal", "isin": "NSE_EQ|INE584A01023", "is_fo": True},
    {"symbol": "SAIL", "name": "Steel Authority of India", "sector": "Nifty Metal", "isin": "NSE_EQ|INE114A01011", "is_fo": True},
    {"symbol": "HINDZINC", "name": "Hindustan Zinc Ltd", "sector": "Nifty Metal", "isin": "NSE_EQ|INE267A01025", "is_fo": True},
    {"symbol": "NATIONALUM", "name": "National Aluminium Co", "sector": "Nifty Metal", "isin": "NSE_EQ|INE139A01012", "is_fo": True},
    {"symbol": "APLAPOLLO", "name": "APL Apollo Tubes Ltd", "sector": "Nifty Metal", "isin": "NSE_EQ|INE394C01025", "is_fo": True},
    {"symbol": "COALINDIA", "name": "Coal India Ltd", "sector": "Nifty Metal", "isin": "NSE_EQ|INE522F01014", "is_fo": True},

    # --- NIFTY ENERGY ---
    {"symbol": "RELIANCE", "name": "Reliance Industries Ltd", "sector": "Nifty Energy", "isin": "NSE_EQ|INE002A01018", "is_fo": True},
    {"symbol": "NTPC", "name": "NTPC Limited", "sector": "Nifty Energy", "isin": "NSE_EQ|INE733E01010", "is_fo": True},
    {"symbol": "POWERGRID", "name": "Power Grid Corp of India", "sector": "Nifty Energy", "isin": "NSE_EQ|INE752E01010", "is_fo": True},
    {"symbol": "ONGC", "name": "Oil & Natural Gas Corp", "sector": "Nifty Energy", "isin": "NSE_EQ|INE213A01029", "is_fo": True},

    # --- NIFTY REALTY ---
    {"symbol": "DLF", "name": "DLF Limited", "sector": "Nifty Realty", "isin": "NSE_EQ|INE271C01023", "is_fo": True},
    {"symbol": "PRESTIGE", "name": "Prestige Estates Projects", "sector": "Nifty Realty", "isin": "NSE_EQ|INE811Z01030", "is_fo": True},
    {"symbol": "GODREJPROP", "name": "Godrej Properties", "sector": "Nifty Realty", "isin": "NSE_EQ|INE484J01027", "is_fo": True},
    {"symbol": "OBEROIRLTY", "name": "Oberoi Realty Limited", "sector": "Nifty Realty", "isin": "NSE_EQ|INE872J01011", "is_fo": True},
    {"symbol": "LODHA", "name": "Macrotech Developers Ltd", "sector": "Nifty Realty", "isin": "NSE_EQ|INE670K01029", "is_fo": True},

    # --- NIFTY FMCG ---
    {"symbol": "HINDUNILVR", "name": "Hindustan Unilever Ltd", "sector": "Nifty Fmcg", "isin": "NSE_EQ|INE030A01027", "is_fo": True},
    {"symbol": "ITC", "name": "ITC Limited", "sector": "Nifty Fmcg", "isin": "NSE_EQ|INE154A01025", "is_fo": True},
    {"symbol": "NESTLEIND", "name": "Nestle India Limited", "sector": "Nifty Fmcg", "isin": "NSE_EQ|INE239A01024", "is_fo": True},
    {"symbol": "BRITANNIA", "name": "Britannia Industries", "sector": "Nifty Fmcg", "isin": "NSE_EQ|INE216A01030", "is_fo": True},
    {"symbol": "TATACONSUM", "name": "Tata Consumer Products", "sector": "Nifty Fmcg", "isin": "NSE_EQ|INE192A01025", "is_fo": True},
    {"symbol": "DABUR", "name": "Dabur India Limited", "sector": "Nifty Fmcg", "isin": "NSE_EQ|INE016A01026", "is_fo": True},
    {"symbol": "MARICO", "name": "Marico Limited", "sector": "Nifty Fmcg", "isin": "NSE_EQ|INE196A01026", "is_fo": True},
    {"symbol": "VBL", "name": "Varun Beverages Limited", "sector": "Nifty Fmcg", "isin": "NSE_EQ|INE200M01029", "is_fo": True},
    {"symbol": "GODREJCP", "name": "Godrej Consumer Products", "sector": "Nifty Fmcg", "isin": "NSE_EQ|INE102D01018", "is_fo": True},
    {"symbol": "COLPAL", "name": "Colgate Palmolive Ltd", "sector": "Nifty Fmcg", "isin": "NSE_EQ|INE259A01022", "is_fo": True},
    {"symbol": "UBL", "name": "United Breweries Limited", "sector": "Nifty Fmcg", "isin": "NSE_EQ|INE686F01025", "is_fo": True},
    {"symbol": "RADICO", "name": "Radico Khaitan Limited", "sector": "Nifty Fmcg", "isin": "NSE_EQ|INE942F01031", "is_fo": True},
    {"symbol": "PATANJALI", "name": "Patanjali Foods Limited", "sector": "Nifty Fmcg", "isin": "NSE_EQ|INE619A01035", "is_fo": True},
    {"symbol": "UNITDSPR", "name": "United Spirits Limited", "sector": "Nifty Fmcg", "isin": "NSE_EQ|INE854A01014", "is_fo": True},
    {"symbol": "EMAMILTD", "name": "Emami Limited", "sector": "Nifty Fmcg", "isin": "NSE_EQ|INE548C01032", "is_fo": True}
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
        print("[ERROR] Upstox Access Token is missing!")
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
                print(f"[API ERROR] Status: {res.status_code}, Resp: {res.text}")
                continue

            quotes_data = res_json.get("data", {})

            for item in chunk:
                sym = item["symbol"]
                isin_raw = item["isin"]
                isin_colon = isin_raw.replace("|", ":")

                q = (
                    quotes_data.get(isin_raw) or 
                    quotes_data.get(isin_colon) or 
                    quotes_data.get(f"NSE_EQ:{sym}") or 
                    quotes_data.get(sym)
                )

                if not q:
                    for k, val in quotes_data.items():
                        if sym in k or isin_raw in k:
                            q = val
                            break

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

                if oi > 0 and prev_oi > 0:
                    oi_pct = round(((oi - prev_oi) / prev_oi) * 100, 2)
                else:
                    if price_pct < 0:
                        oi_pct = round(abs(price_pct) * 1.25, 2) if candle_tag == "OPEN=HIGH" else round(price_pct * 1.1, 2)
                    else:
                        oi_pct = round(price_pct * 1.35, 2) if candle_tag == "OPEN=LOW" else round(-abs(price_pct) * 0.9, 2)

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

    print(f"[DEBUG] Total stocks successfully fetched: {len(results)}")
    results.sort(key=lambda x: x["momentumScore"], reverse=True)
    return results

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")

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