
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
import streamlit as st
import yfinance as yf

st.set_page_config(page_title="Scanner 7.3 FULL", layout="wide")

st.markdown("""
<style>
:root { --bg1:#081225; --bg2:#0d1b3a; --border:rgba(148,163,184,0.20); }
.block-container { max-width:1320px; padding-top:1rem; padding-bottom:2rem; }
.badge { display:inline-block; border-radius:999px; padding:0.32rem 0.78rem; color:white; font-size:0.76rem; font-weight:800; white-space:nowrap; }
.dark-card { background:linear-gradient(180deg,var(--bg1) 0%,var(--bg2) 100%); border:1px solid var(--border); border-radius:18px; padding:16px; margin-bottom:10px; color:white; box-shadow:0 8px 22px rgba(15,23,42,0.12); }
.info-card { background:linear-gradient(180deg,var(--bg1) 0%,var(--bg2) 100%); border:1px solid var(--border); border-radius:18px; padding:14px 16px; margin-bottom:12px; color:white; }
.info-red { border-color:rgba(220,38,38,0.45); }
.info-blue { border-color:rgba(37,99,235,0.45); }
.event-box { background:#eff6ff; color:#1e3a8a; border:1px solid #dbeafe; border-radius:10px; padding:0.6rem 0.8rem; margin:0.4rem 0 0.8rem 0; font-size:0.9rem; }
.card-top { display:flex; justify-content:space-between; align-items:flex-start; gap:10px; }
.metrics-grid { display:grid; grid-template-columns:1fr 1fr; gap:14px; margin-top:14px; }
.card-col { font-size:0.95rem; line-height:1.65; }
.action-row { display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-top:8px; }
@media (max-width: 900px) {
    .metrics-grid { grid-template-columns:1fr; }
    .action-row { grid-template-columns:1fr; }
}
</style>
""", unsafe_allow_html=True)

APP_TITLE = "🚀 Scanner 7.3 FULL"
FINNHUB_API_KEY = st.secrets["FINNHUB_API_KEY"]

ASSETS: Dict[str, Dict[str, str]] = {
    "AAPL": {"name": "Apple", "wkn": "865985", "type": "Aktie"},
    "MSFT": {"name": "Microsoft", "wkn": "870747", "type": "Aktie"},
    "NVDA": {"name": "NVIDIA", "wkn": "918422", "type": "Aktie"},
    "XOM": {"name": "Exxon Mobil", "wkn": "852549", "type": "Aktie"},
    "AMD": {"name": "Advanced Micro Devices", "wkn": "863186", "type": "Aktie"},
    "ASML": {"name": "ASML ADR", "wkn": "A1J4U4", "type": "Aktie"},
    "CAT": {"name": "Caterpillar", "wkn": "850598", "type": "Aktie"},
    "DTE.DE": {"name": "Deutsche Telekom", "wkn": "555750", "type": "Aktie"},
    "GC=F": {"name": "Gold", "wkn": "-", "type": "Rohstoff"},
    "SI=F": {"name": "Silber", "wkn": "-", "type": "Rohstoff"},
    "CL=F": {"name": "WTI Öl", "wkn": "-", "type": "Rohstoff"},
    "BZ=F": {"name": "Brent Öl", "wkn": "-", "type": "Rohstoff"},
    "NG=F": {"name": "Erdgas", "wkn": "-", "type": "Rohstoff"},
    "BTC-USD": {"name": "Bitcoin", "wkn": "-", "type": "Krypto"},
    "ETH-USD": {"name": "Ethereum", "wkn": "-", "type": "Krypto"},
    "SOL-USD": {"name": "Solana", "wkn": "-", "type": "Krypto"},
}

BENCHMARK_SYMBOL = "URTH"
EURUSD_SYMBOL = "EURUSD=X"

DATA_DIR = Path("data")
PORTFOLIO_FILE = DATA_DIR / "scanner_7_3_full_portfolio.csv"
WATCHLIST_FILE = DATA_DIR / "scanner_7_3_watchlist.csv"
PORTFOLIO_COLUMNS = ["Symbol", "Name", "WKN", "Typ", "Kaufdatum", "Kaufkurs €", "Stück", "Stop €", "Ziel €", "Notiz"]

def ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def load_portfolio():
    ensure_data_dir()
    if not PORTFOLIO_FILE.exists():
        return pd.DataFrame(columns=PORTFOLIO_COLUMNS)
    try:
        df = pd.read_csv(PORTFOLIO_FILE)
    except Exception:
        return pd.DataFrame(columns=PORTFOLIO_COLUMNS)
    for c in PORTFOLIO_COLUMNS:
        if c not in df.columns:
            df[c] = None
    return df[PORTFOLIO_COLUMNS].copy()

def save_portfolio(df):
    ensure_data_dir()
    out = df.copy()
    for c in PORTFOLIO_COLUMNS:
        if c not in out.columns:
            out[c] = None
    out[PORTFOLIO_COLUMNS].to_csv(PORTFOLIO_FILE, index=False)

def load_watchlist():
    ensure_data_dir()
    if not WATCHLIST_FILE.exists():
        return pd.DataFrame(columns=["Symbol", "Name", "Typ"])
    try:
        df = pd.read_csv(WATCHLIST_FILE)
    except Exception:
        return pd.DataFrame(columns=["Symbol", "Name", "Typ"])
    for c in ["Symbol", "Name", "Typ"]:
        if c not in df.columns:
            df[c] = None
    return df[["Symbol", "Name", "Typ"]].copy()

def save_watchlist(df):
    ensure_data_dir()
    out = df.copy()
    for c in ["Symbol", "Name", "Typ"]:
        if c not in out.columns:
            out[c] = None
    out[["Symbol", "Name", "Typ"]].to_csv(WATCHLIST_FILE, index=False)

def add_to_watchlist(row):
    df = load_watchlist()
    symbol = str(row["Symbol"])
    if symbol not in df["Symbol"].astype(str).values:
        new = pd.DataFrame([{
            "Symbol": symbol,
            "Name": str(row["Name"]),
            "Typ": str(row.get("Typ", "")),
        }])
        df = pd.concat([df, new], ignore_index=True)
        save_watchlist(df)

def normalize_df(df):
    if df is None or df.empty:
        return pd.DataFrame()
    result = df.copy()
    if isinstance(result.columns, pd.MultiIndex):
        result.columns = [c[0] if isinstance(c, tuple) else c for c in result.columns]
    rename_map = {}
    for col in result.columns:
        low = str(col).strip().lower()
        if low == "open": rename_map[col] = "Open"
        elif low == "high": rename_map[col] = "High"
        elif low == "low": rename_map[col] = "Low"
        elif low == "close": rename_map[col] = "Close"
        elif low == "adj close": rename_map[col] = "Adj Close"
        elif low == "volume": rename_map[col] = "Volume"
    result = result.rename(columns=rename_map)
    keep = [c for c in ["Open", "High", "Low", "Close", "Adj Close", "Volume"] if c in result.columns]
    result = result[keep].copy()
    for c in result.columns:
        result[c] = pd.to_numeric(result[c], errors="coerce")
    if "Close" not in result.columns:
        return pd.DataFrame()
    return result.dropna(subset=["Close"]).sort_index()

def as_series(obj):
    if isinstance(obj, pd.DataFrame):
        if obj.shape[1] == 0:
            return pd.Series(dtype=float)
        return pd.to_numeric(obj.iloc[:, 0], errors="coerce")
    return pd.to_numeric(obj, errors="coerce")

def safe_float(v):
    try:
        if v is None or pd.isna(v):
            return None
        return float(v)
    except Exception:
        return None

def clamp(value, low, high):
    return max(low, min(high, float(value)))

def fmt_eur(v):
    if v is None or pd.isna(v):
        return "-"
    return f"{float(v):.2f} €"

def fmt_num(v):
    if v is None or pd.isna(v):
        return "-"
    return f"{float(v):.2f}"

def is_eur_symbol(symbol):
    return symbol.endswith((".DE", ".PA", ".AS", ".MI", ".MC", ".BR", ".VI"))

def yahoo_link(symbol):
    return f"https://finance.yahoo.com/quote/{symbol}"

def yahoo_analysis_link(symbol):
    return f"https://finance.yahoo.com/quote/{symbol}/analysis"

def onvista_link(wkn):
    return "#" if not wkn or wkn == "-" else f"https://www.onvista.de/suche?searchValue={wkn}"

@st.cache_data(show_spinner=False, ttl=1800)
def get_fx_eurusd():
    try:
        df = yf.download(EURUSD_SYMBOL, period="10d", interval="1d", progress=False, auto_adjust=False, threads=False)
        df = normalize_df(df)
        close = as_series(df["Close"]).dropna()
        if close.empty:
            return 1.0
        val = safe_float(close.iloc[-1])
        return val if val and val > 0 else 1.0
    except Exception:
        return 1.0

def to_eur(price, symbol):
    if price is None:
        return None
    if is_eur_symbol(symbol):
        return price
    fx = get_fx_eurusd()
    return price / fx if fx else price

def from_eur_to_native(value_eur, symbol):
    if value_eur is None:
        return None
    if is_eur_symbol(symbol):
        return value_eur
    fx = get_fx_eurusd()
    return value_eur * fx if fx else value_eur

def fmt_dual_price(value_eur, symbol):
    if value_eur is None or pd.isna(value_eur):
        return "-"
    native = from_eur_to_native(value_eur, symbol)
    if native is None:
        return fmt_eur(value_eur)
    return f"{float(value_eur):.2f} € ({float(native):.2f} $)"

def risk_reward_ratio(price_eur, stop_eur, target_eur, trend):
    if None in (price_eur, stop_eur, target_eur):
        return None
    if trend == "Long":
        risk = price_eur - stop_eur
        reward = target_eur - price_eur
    elif trend == "Short":
        risk = stop_eur - price_eur
        reward = price_eur - target_eur
    else:
        return None
    if risk is None or reward is None or risk <= 0:
        return None
    return reward / risk

def calculate_position_size(account_size, risk_pct, entry_eur, stop_eur):
    if None in (account_size, risk_pct, entry_eur, stop_eur):
        return (None, None, None)
    risk_amount = account_size * (risk_pct / 100.0)
    risk_per_unit = abs(entry_eur - stop_eur)
    if risk_per_unit <= 0:
        return (risk_amount, None, None)
    units = risk_amount / risk_per_unit
    position_value = units * entry_eur if entry_eur is not None else None
    return (risk_amount, units, position_value)

@st.cache_data(show_spinner=False, ttl=1800)
def get_history(symbol, period="1y", interval="1d"):
    try:
        return normalize_df(yf.download(symbol, period=period, interval=interval, progress=False, auto_adjust=False, threads=False))
    except Exception:
        return pd.DataFrame()

def finnhub_available():
    return FINNHUB_API_KEY.strip() not in ("", "DEIN_FINNHUB_API_KEY_HIER")

@st.cache_data(show_spinner=False, ttl=120)
def finnhub_get_quote(symbol):
    if not finnhub_available():
        return {}
    try:
        r = requests.get("https://finnhub.io/api/v1/quote", params={"symbol": symbol, "token": FINNHUB_API_KEY}, timeout=8)
        return r.json() if r.ok else {}
    except Exception:
        return {}

@st.cache_data(show_spinner=False, ttl=1800)
def finnhub_get_news(symbol):
    if not finnhub_available():
        return []
    try:
        to_dt = datetime.now(timezone.utc).date()
        from_dt = to_dt - timedelta(days=5)
        r = requests.get("https://finnhub.io/api/v1/company-news", params={"symbol": symbol, "from": str(from_dt), "to": str(to_dt), "token": FINNHUB_API_KEY}, timeout=8)
        data = r.json() if r.ok else []
        return data if isinstance(data, list) else []
    except Exception:
        return []

@st.cache_data(show_spinner=False, ttl=1800)
def finnhub_get_recommendation(symbol):
    if not finnhub_available():
        return []
    try:
        r = requests.get("https://finnhub.io/api/v1/stock/recommendation", params={"symbol": symbol, "token": FINNHUB_API_KEY}, timeout=8)
        data = r.json() if r.ok else []
        return data if isinstance(data, list) else []
    except Exception:
        return []

@st.cache_data(show_spinner=False, ttl=1800)
def finnhub_get_earnings(symbol):
    if not finnhub_available():
        return []
    try:
        today = datetime.now(timezone.utc).date()
        future = today + timedelta(days=7)
        r = requests.get("https://finnhub.io/api/v1/calendar/earnings", params={"from": str(today), "to": str(future), "symbol": symbol, "token": FINNHUB_API_KEY}, timeout=8)
        data = r.json() if r.ok else {}
        return data.get("earningsCalendar", []) if isinstance(data, dict) else []
    except Exception:
        return []

@st.cache_data(show_spinner=False, ttl=1800)
def load_symbol_data(symbol):
    return {
        "history": get_history(symbol),
        "quote": finnhub_get_quote(symbol),
        "news": finnhub_get_news(symbol),
        "recommendation": finnhub_get_recommendation(symbol),
        "earnings": finnhub_get_earnings(symbol),
    }

def ema(series, period):
    s = as_series(series).dropna()
    return s.ewm(span=period, adjust=False).mean()

def rsi(series, period=14):
    s = as_series(series).dropna()
    delta = s.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    out = 100 - (100 / (1 + rs))
    return out.fillna(50)

def momentum_pct(series, lookback):
    s = as_series(series).dropna()
    if len(s) <= lookback:
        return None
    start = safe_float(s.iloc[-lookback - 1]); end = safe_float(s.iloc[-1])
    if start is None or end is None or start == 0:
        return None
    return ((end / start) - 1.0) * 100.0

def atr(df, period=14):
    if df.empty or len(df) < period + 1:
        return None
    high = as_series(df["High"]).dropna(); low = as_series(df["Low"]).dropna(); close = as_series(df["Close"]).dropna()
    prev_close = close.shift(1)
    tr = pd.concat([(high - low).abs(), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    out = tr.rolling(period).mean()
    return safe_float(out.iloc[-1]) if not out.empty else None

def volume_factor(df):
    if "Volume" not in df.columns:
        return None
    vol = as_series(df["Volume"]).dropna()
    if len(vol) < 21:
        return None
    avg = vol.tail(20).mean(); last = safe_float(vol.iloc[-1])
    if avg == 0 or last is None:
        return None
    return float(last / avg)

def breakout(df):
    close = as_series(df["Close"]).dropna()
    if len(close) < 55:
        return False
    return bool(safe_float(close.iloc[-1]) > safe_float(close.iloc[-51:-1].max()))

def breakdown(df):
    close = as_series(df["Close"]).dropna()
    if len(close) < 55:
        return False
    return bool(safe_float(close.iloc[-1]) < safe_float(close.iloc[-51:-1].min()))

def near_level(price, level, tolerance=0.02):
    if price is None or level is None or level == 0:
        return False
    return abs(price / level - 1.0) <= tolerance

def trend_channel_position(close, lookback=20):
    s = as_series(close).dropna()
    if len(s) < lookback:
        return "-"
    upper = safe_float(s.tail(lookback).max()); lower = safe_float(s.tail(lookback).min()); current = safe_float(s.iloc[-1])
    if None in (upper, lower, current) or upper == lower:
        return "-"
    pos = (current - lower) / (upper - lower)
    if pos <= 0.33: return "unten"
    if pos >= 0.67: return "oben"
    return "mitte"

def calc_relative_strength(close, bench_close, lookback=63):
    s = as_series(close).rename("s"); b = as_series(bench_close).rename("b")
    joined = pd.concat([s, b], axis=1).dropna()
    if len(joined) <= lookback:
        return None
    s1 = safe_float(joined["s"].iloc[-lookback - 1]); s2 = safe_float(joined["s"].iloc[-1]); b1 = safe_float(joined["b"].iloc[-lookback - 1]); b2 = safe_float(joined["b"].iloc[-1])
    if None in (s1, s2, b1, b2) or min(s1, s2, b1, b2) <= 0:
        return None
    diff = ((s2 / s1) - 1) - ((b2 / b1) - 1)
    return clamp(50 + diff * 250, 0, 100)

def score_bucket(value, low, high):
    if value is None or high == low:
        return 50.0
    return clamp((value - low) / (high - low) * 100.0, 0, 100)

def deutsches_signal(signal):
    return {"STRONG BUY": "Kaufsignal", "STRONG SHORT": "Short-Signal", "SETUP": "Beobachten", "NO TRADE": "Kein Signal"}.get(signal, signal)

def badge_color(signal):
    return {"STRONG BUY": "#16a34a", "STRONG SHORT": "#dc2626", "SETUP": "#d97706"}.get(signal, "#64748b")

def fear_greed_label(value):
    if value < 25: return "Extreme Angst"
    if value < 45: return "Angst"
    if value < 55: return "Neutral"
    if value < 75: return "Gier"
    return "Extreme Gier"

def calc_fear_greed(scan_df):
    if scan_df.empty:
        return 50.0, "Neutral"
    score_mean = safe_float(scan_df["Swing-Score Event"].mean()) or 50.0
    rsi_mean = safe_float(scan_df["RSI"].mean()) or 50.0
    rs_mean = safe_float(scan_df["Relative Stärke"].mean()) or 50.0
    vol_mean = safe_float(scan_df["Volumenfaktor"].mean()) or 1.0
    val = score_mean * 0.40 + rsi_mean * 0.15 + rs_mean * 0.25 + clamp(vol_mean * 50, 0, 100) * 0.20
    val = round(clamp(val, 0, 100), 1)
    return val, fear_greed_label(val)

def entry_text(signal, long_trend, short_trend, is_breakout, is_breakdown, pullback_long, pullback_short):
    if is_breakout and long_trend: return "Ausbruch Long"
    if pullback_long and long_trend: return "Rücksetzer Long"
    if is_breakdown and short_trend: return "Ausbruch Short"
    if pullback_short and short_trend: return "Rücksetzer Short"
    if signal == "SETUP" and long_trend: return "Trendaufbau Long"
    if signal == "SETUP" and short_trend: return "Trendaufbau Short"
    return "Noch kein sauberer Einstieg"

POSITIVE_NEWS_WORDS = ["beats", "beat", "surges", "upgrade", "raises", "strong", "record", "wins", "approval", "outperform", "buyback", "partnership"]
NEGATIVE_NEWS_WORDS = ["misses", "miss", "downgrade", "cuts", "lawsuit", "probe", "warning", "falls", "drop", "weaker", "sell", "hold"]

def get_realtime_price_eur(symbol, data):
    q = data.get("quote", {})
    current = safe_float(q.get("c")) if q else None
    if current is not None and current > 0:
        return to_eur(current, symbol)
    hist = data.get("history", pd.DataFrame())
    if hist.empty:
        return None
    close = as_series(hist["Close"]).dropna()
    if close.empty:
        return None
    return to_eur(safe_float(close.iloc[-1]), symbol)

def get_event_overlay(symbol, data):
    overlay = {"earnings_warning": "", "earnings_today": False, "news_note": "", "news_url": "", "analyst_note": "", "analyst_url": yahoo_analysis_link(symbol), "score_adjustment": 0, "position_size_note": "", "event_data_status": ""}
    used_any = False
    earnings = data.get("earnings", [])
    if earnings:
        used_any = True
        try:
            e_date = pd.to_datetime(earnings[0].get("date")).date()
            today = datetime.now(timezone.utc).date()
            delta = (e_date - today).days
            if delta == 0:
                overlay["earnings_today"] = True
                overlay["earnings_warning"] = "Quartalszahlen heute"
                overlay["score_adjustment"] -= 12
                overlay["position_size_note"] = "Berichtssaison heute: Positionsgröße reduzieren."
            elif 0 < delta <= 3:
                overlay["earnings_warning"] = f"Quartalszahlen in {delta} Tagen"
                overlay["score_adjustment"] -= 5
        except Exception:
            pass
    recs = data.get("recommendation", [])
    if recs:
        used_any = True
        try:
            latest = recs[0]
            positive = int(latest.get("buy", 0) or 0) + int(latest.get("strongBuy", 0) or 0)
            negative = int(latest.get("hold", 0) or 0) + int(latest.get("sell", 0) or 0) + int(latest.get("strongSell", 0) or 0)
            if negative > positive:
                overlay["analyst_note"] = "Analysten eher schwächer / Downgrade-Risiko"
                overlay["score_adjustment"] -= 8
            elif positive > negative:
                overlay["analyst_note"] = "Analysten eher positiv"
                overlay["score_adjustment"] += 4
        except Exception:
            pass
    news = data.get("news", [])
    if news:
        used_any = True
        try:
            titles = " ".join(str(i.get("headline", "")).lower() for i in news[:8])
            first_url = ""
            for item in news[:8]:
                candidate = str(item.get("url", "")).strip()
                if candidate:
                    first_url = candidate
                    break
            if first_url:
                overlay["news_url"] = first_url
            pos_hits = sum(1 for w in POSITIVE_NEWS_WORDS if w in titles)
            neg_hits = sum(1 for w in NEGATIVE_NEWS_WORDS if w in titles)
            if pos_hits > neg_hits and pos_hits > 0:
                overlay["news_note"] = "Frische positive News"
                overlay["score_adjustment"] += 5
            elif neg_hits > pos_hits and neg_hits > 0:
                overlay["news_note"] = "Frische negative News"
                overlay["score_adjustment"] -= 5
        except Exception:
            pass
    overlay["event_data_status"] = "Event-Daten aktiv" if used_any else "Keine Event-Daten verfügbar"
    return overlay

@st.cache_data(show_spinner=False, ttl=1800)
def get_benchmark_close():
    df = get_history(BENCHMARK_SYMBOL)
    return as_series(df["Close"]).dropna() if not df.empty else pd.Series(dtype=float)

def analyze(symbol, meta, bench_close):
    data = load_symbol_data(symbol)
    df = data["history"]
    if df.empty or len(df) < 220:
        return None
    close = as_series(df["Close"]).dropna()
    if len(close) < 220:
        return None
    price_native = safe_float(close.iloc[-1])
    ema50_series = ema(close, 50); ema200_series = ema(close, 200)
    ema50 = safe_float(ema50_series.iloc[-1]) if len(ema50_series) else None
    ema200 = safe_float(ema200_series.iloc[-1]) if len(ema200_series) else None
    rsi_val = safe_float(rsi(close).iloc[-1]) if len(close) else None
    mom3 = momentum_pct(close, 63); mom6 = momentum_pct(close, 126)
    vol = volume_factor(df); rs = calc_relative_strength(close, bench_close, 63)
    is_bo = breakout(df); is_bd = breakdown(df); kanal = trend_channel_position(close, 20)
    price_eur = get_realtime_price_eur(symbol, data); atr_eur = to_eur(atr(df, 14), symbol); ema50_eur = to_eur(ema50, symbol)
    long_trend = bool(price_native is not None and ema50 is not None and ema200 is not None and price_native > ema50 > ema200)
    short_trend = bool(price_native is not None and ema50 is not None and ema200 is not None and price_native < ema50 < ema200)
    prev_close = safe_float(close.iloc[-2]) if len(close) >= 2 else None
    pull_long = near_level(price_native, ema50, 0.02) and prev_close is not None and price_native > prev_close
    pull_short = near_level(price_native, ema50, 0.02) and prev_close is not None and price_native < prev_close
    kanal_score = 100.0 if (kanal == "unten" and long_trend) or (kanal == "oben" and short_trend) else 60.0 if kanal == "mitte" else 50.0
    base_swing = (100 if (long_trend or short_trend) else 0) * 0.26 + score_bucket(abs(mom3) if mom3 is not None else None, 0, 25) * 0.14 + score_bucket(abs(mom6) if mom6 is not None else None, 0, 45) * 0.16 + (rs if rs is not None else 50) * 0.18 + score_bucket(vol, 0.8, 2.0) * 0.10 + (100 if (is_bo or is_bd or pull_long or pull_short) else 0) * 0.08 + kanal_score * 0.08
    base_day = score_bucket(rsi_val, 35, 70) * 0.30 + (100 if (is_bo or is_bd) else 40) * 0.22 + score_bucket(vol, 0.8, 2.0) * 0.20 + (100 if ((pull_long or pull_short) or (prev_close is not None and price_native is not None and price_native != prev_close)) else 50) * 0.12 + kanal_score * 0.16
    event = get_event_overlay(symbol, data)
    swing_score = round(clamp(base_swing + event["score_adjustment"], 0, 100), 2)
    day_score = round(clamp(base_day + event["score_adjustment"], 0, 100), 2)

    if long_trend and price_native is not None and ema50 is not None and price_native > ema50 and (is_bo or pull_long) and (rsi_val or 0) >= 55 and (rsi_val or 0) <= 70 and (rs or 0) >= 80 and (vol or 0) >= 1.2:
        signal = "STRONG BUY"
    elif short_trend and price_native is not None and ema50 is not None and price_native < ema50 and (is_bd or pull_short) and (rsi_val or 100) <= 45 and (rs or 100) <= 40:
        signal = "STRONG SHORT"
    elif swing_score >= 60:
        signal = "SETUP"
    else:
        signal = "NO TRADE"

    entry = entry_text(signal, long_trend, short_trend, is_bo, is_bd, pull_long, pull_short)

    if signal in ("STRONG BUY", "SETUP") and long_trend:
        stop = price_eur - 2 * atr_eur if price_eur is not None and atr_eur is not None else None
        ziel = price_eur + 3 * atr_eur if price_eur is not None and atr_eur is not None else None
    elif signal in ("STRONG SHORT", "SETUP") and short_trend:
        stop = price_eur + 2 * atr_eur if price_eur is not None and atr_eur is not None else None
        ziel = price_eur - 3 * atr_eur if price_eur is not None and atr_eur is not None else None
    else:
        stop = None; ziel = None

    rr = risk_reward_ratio(price_eur, stop, ziel, "Long" if long_trend else "Short" if short_trend else "Neutral")
    if rr is not None and rr < 1.5 and signal in ("STRONG BUY", "STRONG SHORT"):
        signal = "SETUP"

    return {
        "Symbol": symbol, "Name": meta["name"], "WKN": meta.get("wkn", "-"), "Typ": meta.get("type", ""),
        "Preis €": price_eur, "EMA50 €": ema50_eur, "RSI": rsi_val, "Volumenfaktor": vol, "Relative Stärke": rs, "Trendkanal": kanal,
        "Signal": signal, "Signal Deutsch": deutsches_signal(signal), "Einstieg": entry, "Trend": "Long" if long_trend else "Short" if short_trend else "Neutral",
        "Swing-Score": round(base_swing, 2), "Day-Score": round(base_day, 2), "Swing-Score Event": swing_score, "Day-Score Event": day_score,
        "Stop €": stop, "Ziel €": ziel, "CRV": rr,
        "Earnings Hinweis": event["earnings_warning"], "News Hinweis": event["news_note"], "News URL": event["news_url"],
        "Analysten Hinweis": event["analyst_note"], "Analysten URL": event["analyst_url"], "Positionsgröße Hinweis": event["position_size_note"], "Event Status": event["event_data_status"]
    }

def analyze_assets(show_crypto):
    bench_close = get_benchmark_close()
    rows = []
    for symbol, meta in ASSETS.items():
        if not show_crypto and meta["type"] == "Krypto":
            continue
        try:
            row = analyze(symbol, meta, bench_close)
            if row:
                rows.append(row)
        except Exception:
            continue
    return pd.DataFrame(rows).sort_values(["Swing-Score Event", "Day-Score Event"], ascending=False).reset_index(drop=True) if rows else pd.DataFrame()

def add_to_portfolio(row):
    df = load_portfolio()
    new_row = {
        "Symbol": row["Symbol"], "Name": row["Name"], "WKN": row["WKN"], "Typ": row["Typ"], "Kaufdatum": str(date.today()),
        "Kaufkurs €": safe_float(row["Preis €"]) or 0.0, "Stück": 1.0, "Stop €": safe_float(row["Stop €"]), "Ziel €": safe_float(row["Ziel €"]), "Notiz": "Aus Signal übernommen"
    }
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    save_portfolio(df)

def portfolio_signal(symbol, buy_price_eur, shares, all_df):
    match = all_df[all_df["Symbol"] == symbol]
    if match.empty:
        return None
    row = match.iloc[0]
    current = row["Preis €"]; stop = row["Stop €"]
    pnl_pct = ((current / buy_price_eur) - 1.0) * 100.0 if current is not None and buy_price_eur else None

    action = "Halten"; reasons = []
    if current is not None and stop is not None and row["Trend"] != "Short" and current < stop:
        action = "SOFORT VERKAUFEN"; reasons.append("Kurs unter ATR-Stop")
    elif row["Signal"] == "STRONG SHORT":
        action = "Verkaufen"; reasons.append("Scanner meldet Short-Signal")
    elif row["Trend"] == "Neutral":
        action = "Verkaufen"; reasons.append("Trend gebrochen")
    elif row["Signal"] == "STRONG BUY" and pnl_pct is not None and pnl_pct < 5:
        action = "Aufstocken"; reasons.append("Starkes Kaufsignal erneut aktiv")
    elif pnl_pct is not None and pnl_pct > 20 and row["RSI"] is not None and row["RSI"] > 75:
        action = "Gewinne sichern"; reasons.append("Hoher Gewinn bei hohem RSI")
    else:
        reasons.append("Trend intakt")
    if row["Earnings Hinweis"]:
        reasons.append(row["Earnings Hinweis"])

    return {"Symbol": symbol, "Aktuell €": current, "Stop €": row["Stop €"], "Ziel €": row["Ziel €"], "CRV": row["CRV"], "P&L %": pnl_pct, "Aktion": action, "Grund": " | ".join(reasons)}

def build_portfolio_signals(all_df):
    df = load_portfolio()
    rows = []
    for _, pos in df.iterrows():
        symbol = str(pos.get("Symbol", "")).strip().upper()
        if not symbol:
            continue
        row = portfolio_signal(symbol, safe_float(pos.get("Kaufkurs €")) or 0.0, safe_float(pos.get("Stück")) or 0.0, all_df)
        if row:
            rows.append(row)
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    order = {"SOFORT VERKAUFEN": 0, "Verkaufen": 1, "Gewinne sichern": 2, "Aufstocken": 3, "Halten": 4}
    out["Sort"] = out["Aktion"].map(order).fillna(9)
    return out.sort_values(["Sort", "P&L %"], ascending=[True, False]).drop(columns=["Sort"]).reset_index(drop=True)

def render_fear_greed(value, label):
    value = max(0.0, min(100.0, float(value)))
    html = '<div style="margin-top:6px;max-width:360px;"><div style="position:relative;"><div style="display:flex;height:18px;border-radius:999px;overflow:hidden;border:1px solid #cbd5e1;background:#fff;"><div style="background:#991b1b;width:20%;"></div><div style="background:#dc2626;width:20%;"></div><div style="background:#d97706;width:20%;"></div><div style="background:#16a34a;width:20%;"></div><div style="background:#166534;width:20%;"></div></div>' + f'<div style="position:absolute;left:calc({value}% - 7px);top:20px;width:0;height:0;border-left:7px solid transparent;border-right:7px solid transparent;border-top:12px solid #0f172a;"></div></div><div style="margin-top:16px;color:#64748b;font-size:0.84rem;">{label}</div></div>'
    st.markdown(html, unsafe_allow_html=True)

def render_card(row, key_suffix):
    left_col = f'Preis: <b>{fmt_dual_price(row["Preis €"], row["Symbol"])}</b><br>Stop: <b>{fmt_dual_price(row["Stop €"], row["Symbol"])}</b><br>Ziel: <b>{fmt_dual_price(row["Ziel €"], row["Symbol"])}</b><br>Trend: <b>{row["Trend"]}</b><br>Einstieg: <b>{row["Einstieg"]}</b>'
    rs_display = float(row["Relative Stärke"]) if pd.notna(row["Relative Stärke"]) else 50.0
    right_col = f'Swing-Score: <b>{fmt_num(row["Swing-Score Event"])}</b><br>Day-Score: <b>{fmt_num(row["Day-Score Event"])}</b><br>RSI: <b>{fmt_num(row["RSI"])}</b><br>Relative Stärke: <b>{rs_display:.2f}</b><br>Trendkanal: <b>{row["Trendkanal"]}</b><br>CRV: <b>{fmt_num(row["CRV"])}</b>'
    html = '<div class="dark-card"><div class="card-top">' + f'<div><div style="font-size:1.35rem;font-weight:800;line-height:1.1;">{row["Symbol"]}</div><div style="color:#cbd5e1;font-size:0.96rem;margin-bottom:0.18rem;">{row["Name"]}</div><div style="font-size:0.84rem;"><a href="{yahoo_link(str(row["Symbol"]))}" target="_blank" style="color:#93c5fd;text-decoration:none;font-weight:700;margin-right:12px;">Chart</a>' + (f'<a href="{onvista_link(str(row["WKN"]))}" target="_blank" style="color:#93c5fd;text-decoration:none;font-weight:700;">WKN {row["WKN"]}</a>' if str(row["WKN"]) != "-" else '<span style="color:#94a3b8;">WKN -</span>') + '</div></div>' + f'<div><span class="badge" style="background:{badge_color(str(row["Signal"]))};">{row["Signal Deutsch"]}</span></div></div><div class="metrics-grid"><div class="card-col">{left_col}</div><div class="card-col">{right_col}</div></div></div>'
    st.markdown(html, unsafe_allow_html=True)

    notes = []
    if row["Earnings Hinweis"]:
        notes.append(f'📅 {row["Earnings Hinweis"]}')
    if row["Positionsgröße Hinweis"]:
        notes.append(f'⚠️ {row["Positionsgröße Hinweis"]}')
    if notes:
        st.markdown(f'<div class="event-box">{" | ".join(notes)}</div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="event-box">ℹ️ {row["Event Status"]}</div>', unsafe_allow_html=True)

    if row["Analysten URL"]:
        st.markdown(f'[🏦 Analysten / Schätzung öffnen]({row["Analysten URL"]})')
    elif row["Analysten Hinweis"]:
        st.markdown(f'🏦 {row["Analysten Hinweis"]}')

    if row["News URL"]:
        st.markdown(f'[📰 Neue News öffnen]({row["News URL"]})')
    elif row["News Hinweis"]:
        st.markdown(f'📰 {row["News Hinweis"]}')

    risk_amount, units, position_value = calculate_position_size(
        st.session_state.get("account_size_value", 10000.0),
        st.session_state.get("risk_pct_value", 1.0),
        safe_float(row["Preis €"]),
        safe_float(row["Stop €"]),
    )
    st.caption(
        f'Positionsgröße: Risiko {fmt_eur(risk_amount)} | Stück {fmt_num(units)} | Positionswert {fmt_eur(position_value)}'
        if risk_amount is not None else 'Positionsgröße: -'
    )

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Ins Portfolio", key=f'add_{key_suffix}_{row["Symbol"]}', use_container_width=True):
            add_to_portfolio(row)
            st.success(f'{row["Symbol"]} wurde ins Portfolio gespeichert.')
            st.rerun()
    with col2:
        if st.button("👁️ Auf Watchlist", key=f'watch_{key_suffix}_{row["Symbol"]}', use_container_width=True):
            add_to_watchlist(row)
            st.success(f'{row["Symbol"]} zur Watchlist hinzugefügt.')
            st.rerun()

if "refresh_counter" not in st.session_state:
    st.session_state.refresh_counter = 0
if "account_size_value" not in st.session_state:
    st.session_state.account_size_value = 10000.0
if "risk_pct_value" not in st.session_state:
    st.session_state.risk_pct_value = 1.0

title_col, refresh_col = st.columns([5, 1.3])
with title_col:
    st.title(APP_TITLE)
    st.caption("Realtime-Kurse in Euro mit Dollar in Klammern, Events, Quick Trade Mode, CRV, Watchlist und Positionsgröße.")
with refresh_col:
    st.write("")
    st.write("")
    if st.button("🔄 Aktualisieren", use_container_width=True):
        st.cache_data.clear()
        st.session_state.refresh_counter += 1
        st.rerun()

settings1, settings2, settings3, settings4 = st.columns(4)
with settings1:
    show_crypto = st.checkbox("Krypto anzeigen", value=False)
with settings2:
    quick_mode = st.toggle("⚡ Nur Top Trades", value=False)
with settings3:
    st.session_state.account_size_value = st.number_input("Kontogröße €", min_value=100.0, value=float(st.session_state.account_size_value), step=100.0)
with settings4:
    st.session_state.risk_pct_value = st.number_input("Risiko je Trade %", min_value=0.1, max_value=10.0, value=float(st.session_state.risk_pct_value), step=0.1)

all_df = analyze_assets(show_crypto)
if quick_mode and not all_df.empty:
    all_df = all_df[all_df["Signal"].isin(["STRONG BUY", "STRONG SHORT"])].reset_index(drop=True)

portfolio_signal_df = build_portfolio_signals(all_df) if not all_df.empty else pd.DataFrame()

if not portfolio_signal_df.empty:
    urgent = portfolio_signal_df[portfolio_signal_df["Aktion"].isin(["SOFORT VERKAUFEN", "Verkaufen"])]
    text = "Es gibt dringende Verkaufssignale. Bitte zuerst prüfen." if not urgent.empty else "Aktuell überwiegend Halten / Gewinne sichern / Aufstocken."
    st.markdown(f'<div class="info-card {"info-red" if not urgent.empty else "info-blue"}"><b>Portfolio zuerst:</b> {text}</div>', unsafe_allow_html=True)
    tmp_port = portfolio_signal_df.head(5).copy()
    tmp_port["Aktuell Anzeige"] = tmp_port.apply(lambda r: fmt_dual_price(r["Aktuell €"], r["Symbol"]), axis=1)
    tmp_port["Stop Anzeige"] = tmp_port.apply(lambda r: fmt_dual_price(r["Stop €"], r["Symbol"]), axis=1)
    tmp_port["Ziel Anzeige"] = tmp_port.apply(lambda r: fmt_dual_price(r["Ziel €"], r["Symbol"]), axis=1)
    st.dataframe(tmp_port[["Symbol", "Aktuell Anzeige", "Stop Anzeige", "Ziel Anzeige", "CRV", "P&L %", "Aktion", "Grund"]], use_container_width=True, hide_index=True)

aktien_df = all_df[all_df["Typ"] == "Aktie"].copy() if not all_df.empty else pd.DataFrame()
rohstoffe_df = all_df[all_df["Typ"] == "Rohstoff"].copy() if not all_df.empty else pd.DataFrame()
krypto_df = all_df[all_df["Typ"] == "Krypto"].copy() if not all_df.empty else pd.DataFrame()

fg_value, fg_label = calc_fear_greed(all_df if not all_df.empty else pd.DataFrame())

c1, c2, c3 = st.columns(3)
with c1:
    st.metric("Fear & Greed", f"{fg_value:.1f}")
    render_fear_greed(fg_value, fg_label)
with c2:
    st.metric("Sentiment", fg_label)
with c3:
    relation = "-"
    if not all_df.empty:
        avg_price = safe_float(all_df["Preis €"].mean()); avg_ema50 = safe_float(all_df["EMA50 €"].mean())
        if avg_price is not None and avg_ema50 is not None:
            relation = "Preis über EMA50" if avg_price > avg_ema50 else "Preis unter EMA50"
    st.metric("Preis / EMA50", relation)

st.markdown("---")

st.subheader("Suche")
search_text = st.text_input("Nach Unternehmensname, Rohstoff oder Krypto suchen", value="")
if search_text.strip():
    mask = all_df["Name"].str.contains(search_text.strip(), case=False, na=False) if not all_df.empty else pd.Series(dtype=bool)
    search_df = all_df[mask].copy() if not all_df.empty else pd.DataFrame()
    if search_df.empty:
        st.info("Kein Treffer.")
    else:
        for idx, row in search_df.head(10).iterrows():
            render_card(row, f"search_{idx}")

st.subheader("Top 3 Aktien")
if aktien_df.empty:
    st.info("Keine Aktien-Treffer.")
else:
    for idx, row in aktien_df.head(3).iterrows():
        render_card(row, f"stock_{idx}")

st.subheader("Top 3 Rohstoffe")
if rohstoffe_df.empty:
    st.info("Keine Rohstoff-Treffer.")
else:
    for idx, row in rohstoffe_df.head(3).iterrows():
        render_card(row, f"comm_{idx}")

if show_crypto:
    st.subheader("Top 3 Kryptowährungen")
    if krypto_df.empty:
        st.info("Keine Krypto-Treffer.")
    else:
        for idx, row in krypto_df.head(3).iterrows():
            render_card(row, f"crypto_{idx}")

with st.expander("👁️ Watchlist (gespeichert)", expanded=False):
    watch_saved = load_watchlist()
    if watch_saved.empty:
        st.info("Noch keine Werte gespeichert.")
    else:
        st.dataframe(watch_saved, use_container_width=True, hide_index=True)
        delete_symbol = st.selectbox("Von Watchlist entfernen", watch_saved["Symbol"])
        if st.button("Entfernen"):
            watch_saved = watch_saved[watch_saved["Symbol"] != delete_symbol]
            save_watchlist(watch_saved)
            st.success("Entfernt")
            st.rerun()

with st.expander("Watchlist", expanded=False):
    if all_df.empty:
        st.info("Noch keine Daten.")
    else:
        watch_df = all_df.copy()
        watch_df["Preis Anzeige"] = watch_df.apply(lambda r: fmt_dual_price(r["Preis €"], r["Symbol"]), axis=1)
        watch_df["Stop Anzeige"] = watch_df.apply(lambda r: fmt_dual_price(r["Stop €"], r["Symbol"]), axis=1)
        watch_df["Ziel Anzeige"] = watch_df.apply(lambda r: fmt_dual_price(r["Ziel €"], r["Symbol"]), axis=1)
        watch_df["Risiko €"] = watch_df.apply(lambda r: calculate_position_size(st.session_state.account_size_value, st.session_state.risk_pct_value, safe_float(r["Preis €"]), safe_float(r["Stop €"]))[0], axis=1)
        watch_df["Stück"] = watch_df.apply(lambda r: calculate_position_size(st.session_state.account_size_value, st.session_state.risk_pct_value, safe_float(r["Preis €"]), safe_float(r["Stop €"]))[1], axis=1)
        st.dataframe(
            watch_df[["Name", "Symbol", "Typ", "Signal Deutsch", "Trend", "Einstieg", "CRV", "Preis Anzeige", "Stop Anzeige", "Ziel Anzeige", "Risiko €", "Stück"]],
            use_container_width=True,
            hide_index=True,
        )
        selected_name = st.selectbox("Aus Watchlist ins Portfolio", watch_df["Name"].tolist())
        if st.button("Ausgewählten Wert ins Portfolio"):
            row = watch_df[watch_df["Name"] == selected_name].iloc[0]
            add_to_portfolio(row)
            st.success(f"{selected_name} wurde ins Portfolio gespeichert.")
            st.rerun()

with st.expander("Positionsgrößen-Rechner", expanded=False):
    st.caption("Berechnung auf Basis von Kontogröße, Risiko je Trade und Abstand zwischen Einstieg und Stop.")
    if all_df.empty:
        st.info("Keine Signale verfügbar.")
    else:
        calc_name = st.selectbox("Wert für Positionsgröße", all_df["Name"].tolist(), key="possize_name")
        calc_row = all_df[all_df["Name"] == calc_name].iloc[0]
        risk_amount, units, position_value = calculate_position_size(
            st.session_state.account_size_value,
            st.session_state.risk_pct_value,
            safe_float(calc_row["Preis €"]),
            safe_float(calc_row["Stop €"]),
        )
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Risiko je Trade", fmt_eur(risk_amount))
        with c2:
            st.metric("Stückzahl", fmt_num(units))
        with c3:
            st.metric("Positionswert", fmt_eur(position_value))
        st.write(f"Preis: {fmt_dual_price(calc_row['Preis €'], calc_row['Symbol'])}")
        st.write(f"Stop: {fmt_dual_price(calc_row['Stop €'], calc_row['Symbol'])}")
        st.write(f"CRV: {fmt_num(calc_row['CRV'])}")

with st.expander("Portfolio Manager", expanded=False):
    st.caption("Positionen bleiben in data/scanner_7_3_full_portfolio.csv gespeichert.")
    portfolio_df = load_portfolio()
    with st.form("portfolio_add_form", clear_on_submit=False):
        a, b, c, d, e, f = st.columns([1.1, 1.2, 1.0, 0.9, 1.0, 1.1])
        sym = a.text_input("Symbol", value="")
        name = b.text_input("Name", value="")
        buy_price = c.number_input("Kaufkurs €", min_value=0.0, value=0.0, step=0.1)
        shares = d.number_input("Stück", min_value=0.0, value=1.0, step=1.0)
        stop_input = e.number_input("Stop €", min_value=0.0, value=0.0, step=0.1)
        target_input = f.number_input("Ziel €", min_value=0.0, value=0.0, step=0.1)
        note = st.text_input("Notiz", value="")
        submitted = st.form_submit_button("Position speichern")
        if submitted and sym.strip():
            sym = sym.strip().upper()
            meta = ASSETS.get(sym, {"name": name or sym, "wkn": "-", "type": ""})
            new_row = {"Symbol": sym, "Name": name.strip() or meta.get("name", sym), "WKN": meta.get("wkn", "-"), "Typ": meta.get("type", ""), "Kaufdatum": str(date.today()), "Kaufkurs €": buy_price, "Stück": shares, "Stop €": stop_input if stop_input > 0 else None, "Ziel €": target_input if target_input > 0 else None, "Notiz": note}
            portfolio_df = pd.concat([portfolio_df, pd.DataFrame([new_row])], ignore_index=True)
            save_portfolio(portfolio_df)
            st.success(f"{sym} wurde gespeichert.")
            st.rerun()
    portfolio_df = load_portfolio()
    if portfolio_df.empty:
        st.info("Noch keine Positionen gespeichert.")
    else:
        show_pf = portfolio_df.copy()
        show_pf["Kaufkurs Anzeige"] = show_pf.apply(lambda r: fmt_dual_price(r["Kaufkurs €"], r["Symbol"]), axis=1)
        show_pf["Stop Anzeige"] = show_pf.apply(lambda r: fmt_dual_price(r["Stop €"], r["Symbol"]), axis=1)
        show_pf["Ziel Anzeige"] = show_pf.apply(lambda r: fmt_dual_price(r["Ziel €"], r["Symbol"]), axis=1)
        st.dataframe(show_pf[["Symbol", "Name", "Typ", "Kaufdatum", "Kaufkurs Anzeige", "Stück", "Stop Anzeige", "Ziel Anzeige", "Notiz"]], use_container_width=True, hide_index=True)
        delete_options = [f"{idx}: {row['Name']} | Kaufkurs {row['Kaufkurs €']} € | Stück {row['Stück']}" for idx, row in portfolio_df.iterrows()]
        selected = st.selectbox("Position löschen", delete_options)
        if st.button("Ausgewählte Position löschen"):
            idx = int(str(selected).split(":")[0])
            portfolio_df = portfolio_df.drop(index=idx).reset_index(drop=True)
            save_portfolio(portfolio_df)
            st.success("Position gelöscht.")
            st.rerun()
