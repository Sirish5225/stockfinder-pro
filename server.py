import os
import requests
import csv
import io
from datetime import datetime
import pytz
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from collections import defaultdict

app = Flask(__name__)
CORS(app)

UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiJDTDgwMDQiLCJqdGkiOiI2YTZlZGU0YTdkMDdkYzI0NTcxY2IwNjgiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlzRXh0ZW5kZWQiOnRydWUsImlhdCI6MTc4NTY1MDc2MiwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxODE3MjQ0MDAwfQ.QevX5BwRdiDzZNmuSc0CGqDZcN5VP1qK6GXbvziEAik").strip()

# Ticker Bar Index Instruments
MARKET_INDICES = [
    {"symbol": "NIFTY 50", "isin": "NSE_INDEX|Nifty 50"},
    {"symbol": "NIFTY BANK", "isin": "NSE_INDEX|Nifty Bank"},
    {"symbol": "SENSEX", "isin": "BSE_INDEX|SENSEX"},
    {"symbol": "NIFTY FIN SERVICE", "isin": "NSE_INDEX|Nifty Financial Services"},
    {"symbol": "NIFTY IT", "isin": "NSE_INDEX|Nifty IT"},
    {"symbol": "NIFTY AUTO", "isin": "NSE_INDEX|Nifty Auto"},
    {"symbol": "NIFTY PHARMA", "isin": "NSE_INDEX|Nifty Pharma"},
    {"symbol": "NIFTY METAL", "isin": "NSE_INDEX|Nifty Metal"},
    {"symbol": "NIFTY ENERGY", "isin": "NSE_INDEX|Nifty Energy"},
    {"symbol": "NIFTY REALTY", "isin": "NSE_INDEX|Nifty Realty"},
    {"symbol": "NIFTY FMCG", "isin": "NSE_INDEX|Nifty FMCG"}
]

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
    {"symbol": "PERSISTENT", "name": "Persistent Systems Ltd", "sector": "Nifty It", "isin": "NSE_EQ|INE262H01021", "is_fo": True},
    {"symbol": "COFORGE", "name": "Coforge Limited", "sector": "Nifty It", "isin": "NSE_EQ|INE591G01017", "is_fo": True},
    {"symbol": "LTTS", "name": "L&T Technology Services Ltd", "sector": "Nifty It", "isin": "NSE_EQ|INE010V01015", "is_fo": True},
    {"symbol": "KPITTECH", "name": "KPIT Technologies Ltd", "sector": "Nifty It", "isin": "NSE_EQ|INE04I01020", "is_fo": True},

    # --- NIFTY AUTO ---
    {"symbol": "SONACOMS", "name": "Sona BLW Precision Forgings Ltd", "sector": "Nifty Auto", "isin": "NSE_EQ|INE0ZZ101019", "is_fo": True},
    {"symbol": "TATAMOTORS", "name": "Tata Motors Ltd", "sector": "Nifty Auto", "isin": "NSE_EQ|INE155A01022", "is_fo": True},
    {"symbol": "MARUTI", "name": "Maruti Suzuki India", "sector": "Nifty Auto", "isin": "NSE_EQ|INE585B01010", "is_fo": True},
    {"symbol": "M&M", "name": "Mahindra & Mahindra Ltd", "sector": "Nifty Auto", "isin": "NSE_EQ|INE101A01026", "is_fo": True},
    {"symbol": "BAJAJ-AUTO", "name": "Bajaj Auto Limited", "sector": "Nifty Auto", "isin": "NSE_EQ|INE917I01010", "is_fo": True},
    {"symbol": "EICHERMOT", "name": "Eicher Motors Ltd", "sector": "Nifty Auto", "isin": "NSE_EQ|INE066A01021", "is_fo": True},
    {"symbol": "HEROMOTOCO", "name": "Hero MotoCorp Ltd", "sector": "Nifty Auto", "isin": "NSE_EQ|INE158A01026", "is_fo": True},
    {"symbol": "TVSMOTOR", "name": "TVS Motor Company Ltd", "sector": "Nifty Auto", "isin": "NSE_EQ|INE494B01023", "is_fo": True},
    {"symbol": "ASHOKLEY", "name": "Ashok Leyland Ltd", "sector": "Nifty Auto", "isin": "NSE_EQ|INE208A01029", "is_fo": True},
    {"symbol": "BOSCHLTD", "name": "Bosch Limited", "sector": "Nifty Auto", "isin": "NSE_EQ|INE323A01026", "is_fo": True},
    {"symbol": "ESCORTS", "name": "Escorts Kubota Limited", "sector": "Nifty Auto", "isin": "NSE_EQ|INE042A01014", "is_fo": True},
    {"symbol": "BALKRISIND", "name": "Balkrishna Industries Ltd", "sector": "Nifty Auto", "isin": "NSE_EQ|INE787D01026", "is_fo": True},
    {"symbol": "MOTHERSON", "name": "Samvardhana Motherson Intl", "sector": "Nifty Auto", "isin": "NSE_EQ|INE775A01035", "is_fo": True},

    # --- NIFTY PHARMA & HEALTHCARE ---
    {"symbol": "SUNPHARMA", "name": "Sun Pharma Industries", "sector": "Nifty Pharma", "isin": "NSE_EQ|INE044A01036", "is_fo": True},
    {"symbol": "CIPLA", "name": "Cipla Limited", "sector": "Nifty Pharma", "isin": "NSE_EQ|INE059A01026", "is_fo": True},
    {"symbol": "DRREDDY", "name": "Dr. Reddy's Laboratories", "sector": "Nifty Pharma", "isin": "NSE_EQ|INE089A01023", "is_fo": True},
    {"symbol": "DIVISLAB", "name": "Divi's Laboratories", "sector": "Nifty Pharma", "isin": "NSE_EQ|INE361B01024", "is_fo": True},
    {"symbol": "LUPIN", "name": "Lupin Limited", "sector": "Nifty Pharma", "isin": "NSE_EQ|INE326A01037", "is_fo": True},
    {"symbol": "AUROPHARMA", "name": "Aurobindo Pharma", "sector": "Nifty Pharma", "isin": "NSE_EQ|INE406A01037", "is_fo": True},
    {"symbol": "TORNTPHARM", "name": "Torrent Pharmaceuticals Ltd", "sector": "Nifty Pharma", "isin": "NSE_EQ|INE685A01028", "is_fo": True},
    {"symbol": "GLENMARK", "name": "Glenmark Pharmaceuticals Ltd", "sector": "Nifty Pharma", "isin": "NSE_EQ|INE935A01035", "is_fo": True},
    {"symbol": "BIOCON", "name": "Biocon Limited", "sector": "Nifty Pharma", "isin": "NSE_EQ|INE376G01013", "is_fo": True},
    {"symbol": "IPCALAB", "name": "IPCA Laboratories Ltd", "sector": "Nifty Pharma", "isin": "NSE_EQ|INE571A01038", "is_fo": True},
    {"symbol": "ALKEM", "name": "Alkem Laboratories Ltd", "sector": "Nifty Pharma", "isin": "NSE_EQ|INE540L01014", "is_fo": True},
    {"symbol": "APOLLOHOSP", "name": "Apollo Hospitals Enterprise", "sector": "Nifty Healthcare", "isin": "NSE_EQ|INE437A01024", "is_fo": True},
    {"symbol": "MAXHEALTH", "name": "Max Healthcare Institute Ltd", "sector": "Nifty Healthcare", "isin": "NSE_EQ|INE275H01029", "is_fo": True},
    {"symbol": "FORTIS", "name": "Fortis Healthcare Limited", "sector": "Nifty Healthcare", "isin": "NSE_EQ|INE061F01013", "is_fo": True},
    {"symbol": "LALPATHLAB", "name": "Dr. Lal PathLabs Ltd", "sector": "Nifty Healthcare", "isin": "NSE_EQ|INE600L01024", "is_fo": True},

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
    {"symbol": "CGPOWER", "name": "CG Power and Industrial Solutions", "sector": "Nifty Energy", "isin": "NSE_EQ|INE402A01034", "is_fo": True},
    {"symbol": "PREMIERENE", "name": "Premier Energies Ltd", "sector": "Nifty Energy", "isin": "NSE_EQ|INE0X9S01015", "is_fo": True},
    {"symbol": "RELIANCE", "name": "Reliance Industries Ltd", "sector": "Nifty Energy", "isin": "NSE_EQ|INE002A01018", "is_fo": True},
    {"symbol": "NTPC", "name": "NTPC Limited", "sector": "Nifty Energy", "isin": "NSE_EQ|INE733E01010", "is_fo": True},
    {"symbol": "POWERGRID", "name": "Power Grid Corp of India", "sector": "Nifty Energy", "isin": "NSE_EQ|INE752E01010", "is_fo": True},
    {"symbol": "ONGC", "name": "Oil & Natural Gas Corp", "sector": "Nifty Energy", "isin": "NSE_EQ|INE213A01029", "is_fo": True},
    {"symbol": "BPCL", "name": "Bharat Petroleum Corp Ltd", "sector": "Nifty Energy", "isin": "NSE_EQ|INE029A01011", "is_fo": True},
    {"symbol": "IOC", "name": "Indian Oil Corporation Ltd", "sector": "Nifty Energy", "isin": "NSE_EQ|INE242A01010", "is_fo": True},
    {"symbol": "ADANIGREEN", "name": "Adani Green Energy Ltd", "sector": "Nifty Energy", "isin": "NSE_EQ|INE364U01010", "is_fo": True},
    {"symbol": "ADANIENT", "name": "Adani Enterprises Ltd", "sector": "Nifty Energy", "isin": "NSE_EQ|INE423A01024", "is_fo": True},
    {"symbol": "TATAPOWER", "name": "Tata Power Company Ltd", "sector": "Nifty Energy", "isin": "NSE_EQ|INE245A01021", "is_fo": True},
    {"symbol": "NHPC", "name": "NHPC Limited", "sector": "Nifty Energy", "isin": "NSE_EQ|INE848E01016", "is_fo": True},
    {"symbol": "SJVN", "name": "SJVN Limited", "sector": "Nifty Energy", "isin": "NSE_EQ|INE002L01015", "is_fo": True},

    # --- NIFTY REALTY ---
    {"symbol": "DLF", "name": "DLF Limited", "sector": "Nifty Realty", "isin": "NSE_EQ|INE271C01023", "is_fo": True},
    {"symbol": "PRESTIGE", "name": "Prestige Estates Projects", "sector": "Nifty Realty", "isin": "NSE_EQ|INE811Z01030", "is_fo": True},
    {"symbol": "GODREJPROP", "name": "Godrej Properties", "sector": "Nifty Realty", "isin": "NSE_EQ|INE484J01027", "is_fo": True},
    {"symbol": "OBEROIRLTY", "name": "Oberoi Realty Limited", "sector": "Nifty Realty", "isin": "NSE_EQ|INE872J01011", "is_fo": True},
    {"symbol": "LODHA", "name": "Macrotech Developers Ltd", "sector": "Nifty Realty", "isin": "NSE_EQ|INE670K01029", "is_fo": True},
    {"symbol": "PHOENIXLTD", "name": "The Phoenix Mills Ltd", "sector": "Nifty Realty", "isin": "NSE_EQ|INE211B01039", "is_fo": True},
    {"symbol": "BRIGADE", "name": "Brigade Enterprises Ltd", "sector": "Nifty Realty", "isin": "NSE_EQ|INE791I01019", "is_fo": True},
    {"symbol": "SOBHA", "name": "Sobha Limited", "sector": "Nifty Realty", "isin": "NSE_EQ|INE671H01015", "is_fo": True},

    # --- NIFTY FMCG ---
    {"symbol": "NYKAA", "name": "FSN E-Commerce Ventures Ltd", "sector": "Nifty Fmcg", "isin": "NSE_EQ|INE388Y01029", "is_fo": True},
    {"symbol": "KALYANKJIL", "name": "Kalyan Jewellers India Ltd", "sector": "Nifty Fmcg", "isin": "NSE_EQ|INE303W01018", "is_fo": True},
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

GLOBAL_MARKET_UNIVERSE = CORE_MARKET_UNIVERSE.copy()

def detect_open_setup(open_p, high_p, low_p):
    if open_p <= 0 or low_p <= 0 or high_p <= 0:
        return None
    # Fixed: Added a small tolerance threshold (0.05% or 0.05) to catch Open=High/Low reliably
    if abs(open_p - low_p) <= 0.05 or abs(open_p - low_p) <= (open_p * 0.0005):
        return "OPEN=LOW"
    elif abs(open_p - high_p) <= 0.05 or abs(open_p - high_p) <= (open_p * 0.0005):
        return "OPEN=HIGH"
    return None

def calculate_oi_buildup(price_pct, oi_pct, is_fo, candle_tag):
    if not is_fo:
        return "Cash Equity"
    # Fixed: Ensure Open=High and negative price action correctly flag as Short Buildup
    if candle_tag == "OPEN=HIGH" or price_pct <= -0.3:
        return "Short Buildup"
    elif candle_tag == "OPEN=LOW" or price_pct >= 0.3:
        return "Long Buildup"
    elif price_pct > 0:
        return "Short Covering"
    elif price_pct < 0:
        return "Long Unwinding"
    return "Neutral"

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

    for i in range(0, len(GLOBAL_MARKET_UNIVERSE), chunk_size):
        chunk = GLOBAL_MARKET_UNIVERSE[i:i + chunk_size]
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
                    oi_pct = abs(price_pct) * 1.25

                buildup = calculate_oi_buildup(price_pct, oi_pct, True, candle_tag)
                momentum_score = compute_aggressive_momentum(price_pct, volume, average_volume, candle_tag)

                results.append({
                    "symbol": sym,
                    "name": item["name"],
                    "sector": item.get("sector", "Others"),
                    "ltp": ltp,
                    "pricePct": price_pct,
                    "volume": volume,
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

@app.route("/")
def index():
    return send_from_directory('.', 'index.html')

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
        
        existing_isin_map = {item["symbol"]: item for item in CORE_MARKET_UNIVERSE}
        
        new_universe = []
        if ',' not in sample_line:
            reader = csv.reader(stream)
            for row in reader:
                if not row or not row[0].strip():
                    continue
                sym = row[0].strip().upper()
                if sym in ["SYMBOL", "TICKER", "STOCK"]:
                    continue
                
                if sym in existing_isin_map:
                    new_universe.append(existing_isin_map[sym])
                else:
                    new_universe.append({
                        "symbol": sym,
                        "name": f"{sym} Stock",
                        "sector": "F&O Universe",
                        "isin": f"NSE_EQ|{sym}",
                        "is_fo": True
                    })
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
                else:
                    new_universe.append({
                        "symbol": sym,
                        "name": cleaned_row.get("name") or sym,
                        "sector": cleaned_row.get("sector") or "F&O Universe",
                        "isin": cleaned_row.get("isin") or f"NSE_EQ|{sym}",
                        "is_fo": True
                    })
            
        if new_universe:
            GLOBAL_MARKET_UNIVERSE = new_universe
            print(f"[SUCCESS] Loaded {len(new_universe)} custom stocks into screener universe.")
            return jsonify({
                "status": "success", 
                "message": f"Successfully loaded {len(new_universe)} stocks into screener universe!"
            })
        else:
            return jsonify({"status": "error", "message": "No valid rows found in the uploaded file."}), 400

    except Exception as e:
        print(f"[UPLOAD ERROR] {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/reset-universe", methods=["POST"])
def reset_universe():
    global GLOBAL_MARKET_UNIVERSE
    GLOBAL_MARKET_UNIVERSE = CORE_MARKET_UNIVERSE.copy()
    print("[SUCCESS] Screener reset to default market universe.")
    return jsonify({
        "status": "success",
        "message": f"Successfully reset to default universe ({len(GLOBAL_MARKET_UNIVERSE)} stocks)!"
    })

@app.route("/api/screener", methods=["GET"])
def screener():
    filter_val = request.args.get("filter", "ALL")
    search_val = request.args.get("search", "").strip().lower()

    stocks = fetch_live_upstox_quotes()

    top_gainers = []
    top_losers = []
    if stocks:
        sorted_by_gain = sorted(stocks, key=lambda x: float(x.get("pricePct", 0.0)), reverse=True)
        top_gainers = [s for s in sorted_by_gain if float(s.get("pricePct", 0.0)) > 0][:20]
        
        sorted_by_loss = sorted(stocks, key=lambda x: float(x.get("pricePct", 0.0)))
        top_losers = [s for s in sorted_by_loss if float(s.get("pricePct", 0.0)) < 0][:20]

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
        "data": stocks,
        "topGainers": top_gainers,
        "topLosers": top_losers
    })

@app.route("/api/ticker-bar", methods=["GET"])
def ticker_bar():
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"
    }
    
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
                        "symbol": idx["symbol"],
                        "price": f"{ltp:,.2f}",
                        "change": f"{net_change:+,.2f} ({price_pct:+.2f}%)",
                        "isPositive": net_change >= 0
                    })
    except Exception as e:
        print(f"[TICKER INDEX ERROR] {e}")

    stocks = fetch_live_upstox_quotes()
    if stocks:
        sorted_by_gain = sorted(stocks, key=lambda x: float(x.get("pricePct", 0.0)), reverse=True)
        if len(sorted_by_gain) > 0:
            top_gainer = sorted_by_gain[0]
            ticker_results.append({
                "symbol": f"TOP GAINER: {top_gainer['symbol']}",
                "price": f"{top_gainer['ltp']:,.2f}",
                "change": f"{top_gainer['pricePct']:+.2f}%",
                "isPositive": True
            })
            
            top_loser = sorted_by_gain[-1]
            ticker_results.append({
                "symbol": f"TOP LOSER: {top_loser['symbol']}",
                "price": f"{top_loser['ltp']:,.2f}",
                "change": f"{top_loser['pricePct']:+.2f}%",
                "isPositive": False
            })

    return jsonify({"status": "success", "data": ticker_results})

@app.route("/api/sectors", methods=["GET"])
def get_sectors():
    stocks = fetch_live_upstox_quotes()
    fo_stocks = [s for s in stocks if s.get("is_fo", True)]
    
    sec_map = defaultdict(lambda: {"stocks": 0, "total_pct": 0.0, "total_vol": 0, "advances": 0, "declines": 0})

    total_market_vol = 0
    total_market_advances = 0
    total_market_declines = 0

    for s in fo_stocks:
        sec = s.get("sector", "Others")
        p_pct = float(s.get("pricePct", 0.0))
        vol = int(s.get("volume", 0))
        
        total_market_vol += vol
        if p_pct >= 0:
            total_market_advances += 1
        else:
            total_market_declines += 1

        sec_map[sec]["stocks"] += 1
        sec_map[sec]["total_pct"] += p_pct
        sec_map[sec]["total_vol"] += vol
        if p_pct >= 0:
            sec_map[sec]["advances"] += 1
        else:
            sec_map[sec]["declines"] += 1

    summary = []
    for sec, val in sec_map.items():
        avg_pct = round(val["total_pct"] / val["stocks"], 2) if val["stocks"] > 0 else 0.0
        summary.append({
            "sector": sec,
            "avgChange": avg_pct,
            "stocksCount": val["stocks"],
            "advances": val["advances"],
            "declines": val["declines"],
            "totalVolume": val["total_vol"]
        })

    summary.sort(key=lambda x: x["avgChange"], reverse=True)
    
    return jsonify({
        "status": "success", 
        "data": summary,
        "marketBreadth": {
            "totalVolume": total_market_vol,
            "advances": total_market_advances,
            "declines": total_market_declines
        }
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
