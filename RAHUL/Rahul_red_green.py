# redgreen_Rahul.py
# -*- coding: utf-8 -*-
"""
Red-Green simplified engine MARKET order + robust LTP plumbing.
Sells all lots at once (single exit by target or SL). CONFIRMS fills via positions/tradeBook.
"""
import os, csv, json, time, threading, traceback, requests
from datetime import datetime, timedelta, time as _time
from typing import Any, Callable
import pandas as pd
import pandas as _pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as _plt
from openpyxl import load_workbook as _load_wb
from openpyxl.drawing.image import Image as _XLImage

# SmartAPI + TOTP
from SmartApi import SmartConnect
import pyotp

# ===================== USER CONFIG =====================
API_KEY      = "rugItMz0"
CLIENT_CODE  = "DIYD93893"
PASSWORD     = "1235"
TOTP_SECRET  = "BU6JOELN73YFX3FLMFKJUHW7TY"

# Telegram
TELEGRAM_BOT_TOKEN = "7956122666:AAGha1NuYZhL2v145JKCAcoE_v3qdF3__AE"
ALLOWED_CHAT_ID = 967501394

# Files
TRADE_LOG_FILE  = "RAHUL/RedGreen_log.csv"
TRADE_LOG_XLSX  = "RAHUL/RedGreen_log_with_chart.xlsx"
STATE_PERSIST_FILE = "RAHUL/Rahul_bot_state.json"
SPOT_SYMBOL  = "BANKNIFTY"
SPOT_TOKEN   = "26009"   # nominal fallback


LOT_SIZE             = 60
TARGET_POINTS        = 20
SL_POINTS            = 18
BROKERAGE_TAX        = 60
SLEEP_INTERVAL       = 0.5
PRE_CLOSE_BUFFER_SEC = 30
DAILY_TRADE_LIMIT    = 20

API_FAIL_COUNT = 0
MAX_API_FAIL = 10
API_FAIL_LOCK = threading.Lock()
API_CIRCUIT_OPEN = False

RUN_FLAG = True
PROGRAM_RUNNING = True

#new code added to fix rate limit and wrong entries
API_MIN_GAP = 0.45  # seconds (≈ 3 calls/sec total)
_last_api_call_ts = 0.0
_api_gap_lock = threading.Lock()

def _throttled_call(fn):
    def wrapped(*a, **k):
        global _last_api_call_ts
        with _api_gap_lock:
            now = time.time()
            wait = API_MIN_GAP - (now - _last_api_call_ts)
            if wait > 0:
                time.sleep(wait)
            _last_api_call_ts = time.time()
        return fn(*a, **k)
    return wrapped


# Locks
API_LOCK = threading.Lock()
FILE_LOCK = threading.Lock()
STATS_LOCK = threading.Lock()
DAY_STATE_LOCK = threading.Lock()

# Stats
STATS = {"total_trades":0,"profit_trades":0,"loss_trades":0,"gross_profit":0.0,"gross_loss":0.0}

# In-memory state
DAY_HAS_TRADE = False
TRADING_ENGINE_ACTIVE = False
BOT_BASELINES = {}
BOT_AVG_ENTRY = {}

# Scrip-master cache
_SCRIP_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
_scrip_master_cache = None
_scrip_master_last_load = None
_scrip_master_lock = threading.Lock()

# ===== Utility & stats helpers =====
def _today_str(): return datetime.now().date().isoformat()
def _entry_window_open() -> bool:
    now_t = datetime.now().time()
    return (_time(0,0) <= now_t < _time(15,30))
def _market_closed():
    cutoff = (datetime.now().replace(hour=15, minute=30, second=0, microsecond=0)
              - timedelta(seconds=PRE_CLOSE_BUFFER_SEC))
    return datetime.now() >= cutoff

def print_trade_summary(prefix: str = "📊 Trade Summary"):
    with STATS_LOCK:
        total  = STATS["total_trades"]
        wins   = STATS["profit_trades"]
        losses = STATS["loss_trades"]
        gprof  = STATS["gross_profit"]
        gloss  = STATS["gross_loss"]
    print(f"\n{prefix}")
    print(f"Total Trades    : {total}")
    print(f"Profitable      : {wins}")
    print(f"Loss Making     : {losses}")
    print(f"🎯 Net PnL      : ₹{(gprof - gloss):.0f}")

def _update_stats_with_pnl(net_pnl: float):
    with STATS_LOCK:
        STATS["total_trades"] += 1
        if net_pnl >= 0:
            STATS["profit_trades"] += 1
            STATS["gross_profit"]  += net_pnl
        else:
            STATS["loss_trades"]   += 1
            STATS["gross_loss"]    += abs(net_pnl)

# Chart / excel helpers
def _build_daily_pnl_chart(csv_path: str, out_png: str = "RAHUL/Rahul_daily_pnls.png") -> bool:
    try:
        df = _pd.read_csv(csv_path)
        expected13 = ["Datetime","Option","Direction","Signal Type","Trigger Price","Entry Price",
                      "Target","SL","Exit Price","Result","PnL","Volume","Expiry"]
        expected14 = expected13 + ["Total PnL"]
        if "Datetime" not in df.columns or "PnL" not in df.columns:
            if df.shape[1] == 14:
                df.columns = expected14
            elif df.shape[1] == 13:
                df.columns = expected13
            else:
                if df.shape[1] > 14:
                    df = df.iloc[:, :14]
                    df.columns = expected14
                else:
                    return False
        df["Datetime"] = _pd.to_datetime(df["Datetime"], errors="coerce")
        df["PnL"] = _pd.to_numeric(df["PnL"], errors="coerce")
        df = df.dropna(subset=["Datetime","PnL"]).copy()
        df["Date"] = df["Datetime"].dt.date
        daily = df.groupby("Date", as_index=False)["PnL"].sum().sort_values("Date")
        _plt.figure(figsize=(8,4))
        vals  = daily["PnL"].values
        dates = daily["Date"].astype(str).values
        bars = _plt.bar(dates, vals, color=["green" if v>=0 else "red" for v in vals])
        _plt.title("Daily PnL")
        _plt.xlabel("Date"); _plt.ylabel("PnL (₹)")
        _plt.xticks(rotation=45, ha="right")
        for b,v in zip(bars, vals):
            _plt.text(b.get_x()+b.get_width()/2, v+(5 if v>=0 else -5), f"{v:.0f}",
                      ha="center", va="bottom" if v>=0 else "top", fontsize=9)
        _plt.tight_layout(); _plt.savefig(out_png, dpi=150); _plt.close()
        return True
    except Exception:
        return False

def _write_csv_to_excel_and_embed_chart(csv_path: str, xlsx_path: str, chart_png: str = "RAHUL/Rahul_daily_pnls.png"):
    try:
        df = _pd.read_csv(csv_path)
    except Exception:
        return
    with _pd.ExcelWriter(xlsx_path, engine="openpyxl", mode="w") as w:
        df.to_excel(w, index=False, sheet_name="Trades")
    try:
        wb = _load_wb(xlsx_path); ws = wb["Trades"]
        if getattr(ws, "_images", None): ws._images.clear()
        img = _XLImage(chart_png)
        ws.add_image(img, "P2")
        wb.save(xlsx_path)
    except Exception:
        pass

def update_excel_with_daily_pnl():
    png = "RAHUL/Rahul_daily_pnls.png"
    if _build_daily_pnl_chart(TRADE_LOG_FILE, png):
        _write_csv_to_excel_and_embed_chart(TRADE_LOG_FILE, TRADE_LOG_XLSX, png)

# ===== Logging helpers =====
os.makedirs(os.path.dirname(TRADE_LOG_FILE) or ".", exist_ok=True)
if not os.path.exists(TRADE_LOG_FILE):
    with open(TRADE_LOG_FILE, "w", newline="") as f:
        csv.writer(f).writerow([
            "Datetime","Option","Direction","Signal Type",
            "Trigger Price","Entry Price","Target","SL",
            "Exit Price","Result","PnL","Volume","Expiry","Total PnL"
        ])

def _parse_row_datetime_safe(dt_str: str):
    if not dt_str:
        return None
    s = str(dt_str).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S","%d-%m-%Y %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    try:
        ts = _pd.to_datetime(s, errors="coerce", dayfirst=True)
        if not _pd.isna(ts):
            return ts.to_pydatetime()
    except Exception:
        pass
    return None

def _to_float_safe(x):
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return 0.0

def _compute_today_pnl_from_csv(csv_path: str) -> float:
    total = 0.0
    today = datetime.now().date()
    if not os.path.exists(csv_path):
        return 0.0
    with FILE_LOCK:
        try:
            with open(csv_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    dt_raw = r.get("Datetime", "") or ""
                    row_dt = _parse_row_datetime_safe(dt_raw)
                    if row_dt and row_dt.date() == today:
                        try:
                            total += _to_float_safe(r.get("PnL", 0))
                        except Exception:
                            pass
        except Exception:
            pass
    return total

def _compute_today_trade_count_from_csv(csv_path: str) -> int:
    count = 0
    today = datetime.now().date()
    if not os.path.exists(csv_path):
        return 0
    with FILE_LOCK:
        try:
            with open(csv_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    row_dt = _parse_row_datetime_safe(r.get("Datetime", ""))
                    if row_dt and row_dt.date() == today:
                        count += 1
        except Exception:
            pass
    return count

def _init_stats_from_csv(csv_path: str):
    try:
        if not os.path.exists(csv_path):
            return
        total = profit = loss = 0
        gross_profit = 0.0
        gross_loss = 0.0
        today = datetime.now().date()
        with FILE_LOCK:
            with open(csv_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    row_dt = _parse_row_datetime_safe(r.get("Datetime", ""))
                    if not row_dt or row_dt.date() != today:
                        continue
                    total += 1
                    pnl = _to_float_safe(r.get("PnL", 0))
                    if pnl >= 0:
                        profit += 1
                        gross_profit += pnl
                    else:
                        loss += 1
                        gross_loss += abs(pnl)
        with STATS_LOCK:
            STATS["total_trades"] = total
            STATS["profit_trades"] = profit
            STATS["loss_trades"] = loss
            STATS["gross_profit"] = gross_profit
            STATS["gross_loss"] = gross_loss
        print(f"[init] seeded STATS from CSV — trades={total}, net_pnl={(gross_profit - gross_loss):.2f}")
    except Exception as e:
        print(f"[init] _init_stats_from_csv failed: {e}")

def _stop_for_the_day(reason: str = ""):
    global RUN_FLAG
    RUN_FLAG = False
    reason = reason.strip()
    msg = f"⛔ Stopping trading for the day. {reason}" if reason else "⛔ Stopping trading for the day."
    try:
        _tg_send(msg)
    except Exception:
        pass
    try:
        update_excel_with_daily_pnl()
        caption = f"📊 EOD Report — {datetime.now().strftime('%Y-%m-%d')} (early stop)"
        if os.path.exists(TRADE_LOG_XLSX):
            _tg_send_document(TRADE_LOG_XLSX, caption + " (Excel)")
        elif os.path.exists(TRADE_LOG_FILE):
            _tg_send_document(TRADE_LOG_FILE, caption + " (CSV)")
    except Exception:
        pass
    print(msg)

# ===== SmartAPI session & robust call wrapper =====
obj = SmartConnect(api_key=API_KEY)
session = None

def _create_session():
    global obj, session
    try:
        totp = pyotp.TOTP(TOTP_SECRET).now()
        session = obj.generateSession(CLIENT_CODE, PASSWORD, totp)
        obj.setSessionExpiryHook(lambda: print("Session expired"))
        print("✅ Login successful / session renewed")
        return True
    except Exception as e:
        print(f"[session] generateSession failed: {e}")
        return False

_create_session()

obj.ltpData = _throttled_call(obj.ltpData)
obj.placeOrder = _throttled_call(obj.placeOrder)
obj.position = _throttled_call(obj.position)
obj.tradeBook = _throttled_call(obj.tradeBook)
obj.orderBook = _throttled_call(obj.orderBook)
obj.getCandleData = _throttled_call(obj.getCandleData)


def _api_call(fn: Callable, *args, retries: int = 5, backoff: float = 1.0,
              allow_refresh: bool = True, **kwargs) -> Any:

    global API_FAIL_COUNT, API_CIRCUIT_OPEN

    attempt = 0
    last_exc = None

    while attempt < retries:
        try:
            result = fn(*args, **kwargs)

            # 🔵 SUCCESS → Reset fail counter
            with API_FAIL_LOCK:
                API_FAIL_COUNT = 0
                API_CIRCUIT_OPEN = False

            return result

        except Exception as e:
            last_exc = e
            attempt += 1
            msg = str(e).lower()

            is_rate_limit = (
                ("exceed" in msg and "rate" in msg)
                or "access denied" in msg
                or "rate limit" in msg
                or "too many" in msg
            )

            print(f"[api_call] attempt {attempt}/{retries} failed for {getattr(fn,'__name__',str(fn))}: {e}")

            if is_rate_limit:
                sleep_for = min(60, backoff * (2 ** (attempt - 1))) + (0.2 * attempt)
                sleep_for = sleep_for * (1.0 + (0.1 * attempt))
                print(f"[api_call] detected rate-limit-like error; backing off {sleep_for:.1f}s before retrying...")
            else:
                sleep_for = min(10, backoff * (2 ** (attempt - 1)))
                print(f"[api_call] backing off {sleep_for:.1f}s before retrying...")

            if allow_refresh and ("session" in msg or "expired" in msg or "ab1004" in msg):
                try:
                    _create_session()
                except Exception:
                    pass

            time.sleep(sleep_for)
            continue

    # 🔴 FINAL FAILURE → Increment circuit breaker
    with API_FAIL_LOCK:
        API_FAIL_COUNT += 1
        print(f"[FAILSAFE] API_FAIL_COUNT = {API_FAIL_COUNT}")

        if API_FAIL_COUNT >= MAX_API_FAIL:
            API_CIRCUIT_OPEN = True
            print("🚨 API CIRCUIT OPENED")

    raise last_exc

def _api_circuit_monitor():
    global RUN_FLAG

    while PROGRAM_RUNNING:
        with API_FAIL_LOCK:
            if API_CIRCUIT_OPEN:
                if RUN_FLAG:
                    print("🚨 API CIRCUIT OPEN — Pausing Trading")
                    RUN_FLAG = False
                    _tg_send("🚨 API Circuit Open. Trading paused.")

        time.sleep(3)


# ===== Telegram helpers =====
def _tg_send(text: str):
    if not TELEGRAM_BOT_TOKEN or not ALLOWED_CHAT_ID:
        return
    try:
        _r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": ALLOWED_CHAT_ID, "text": text},
            timeout=10
        )
    except Exception:
        pass

def _tg_send_document(path: str, caption: str = "") -> bool:
    if not TELEGRAM_BOT_TOKEN or not ALLOWED_CHAT_ID or not os.path.exists(path): return False
    try:
        with open(path, "rb") as fh:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument",
                data={"chat_id": ALLOWED_CHAT_ID, "caption": caption},
                files={"document": (os.path.basename(path), fh)},
                timeout=30
            )
        return True
    except Exception:
        return False

def _tg_send_photo(path: str, caption: str = "") -> bool:
    if not TELEGRAM_BOT_TOKEN or not ALLOWED_CHAT_ID: return False
    if not os.path.exists(path): return False
    try:
        with open(path, "rb") as fh:
            resp = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                data={"chat_id": ALLOWED_CHAT_ID, "caption": caption},
                files={"photo": (os.path.basename(path), fh)},
                timeout=30
            )
        return resp.status_code == 200
    except Exception:
        return False

def _telegram_listener():
    global RUN_FLAG, PROGRAM_RUNNING

    base = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
    # Flush old messages at startup
    try:
        init_resp = requests.get(
            f"{base}/getUpdates",
            params={"timeout": 1},
            timeout=5
        )
        init_data = init_resp.json()
        if "result" in init_data and init_data["result"]:
            offset = init_data["result"][-1]["update_id"] + 1
        else:
            offset = None
    except Exception:
        offset = None
    print("📲 Telegram control armed. Commands: /stop /startbot /status")

    while PROGRAM_RUNNING:
        try:
            resp = requests.get(
                f"{base}/getUpdates",
                params={"timeout": 50, "offset": offset or 0},
                timeout=60
            )

            data = resp.json()

            for upd in data.get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or upd.get("edited_message")
                if not msg:
                    continue

                chat_id = msg.get("chat", {}).get("id")
                text = (msg.get("text") or "").strip().lower()

                if chat_id != ALLOWED_CHAT_ID:
                    continue

                # ======================
                # STOP COMMAND
                # ======================
                if text in ("/stop", "stop", "/kill", "kill"):
                    RUN_FLAG = False
                    PROGRAM_RUNNING = False

                    print("🔴 Telegram: FULL STOP command received.")

                    requests.post(
                        f"{base}/sendMessage",
                        data={
                            "chat_id": chat_id,
                            "text": "🔴 Bot fully stopped."
                        },
                        timeout=10
                    )
                # ======================
                # START COMMAND
                # ======================
                elif text in ("/startbot", "/resume", "resume", "startbot"):
                    RUN_FLAG = True
                    print("🟢 Telegram: RESUME command received.")

                    requests.post(
                        f"{base}/sendMessage",
                        data={
                            "chat_id": chat_id,
                            "text": "🟢 Bot resumed."
                        },
                        timeout=10
                    )

                # ======================
                # STATUS COMMAND
                # ======================
                elif text in ("/status", "status"):

                    ce = ENGINES.get("CE")
                    pe = ENGINES.get("PE")

                    status_msg = "📊 Red-Green Bot Status\n\n"

                    # Bot State
                    status_msg += f"Engine State: {'🟢 Running' if RUN_FLAG else '🔴 Paused'}\n\n"

                    # CE Status
                    if ce and ce.in_position:
                        try:
                            ltp = float(
                                _api_call(lambda: obj.ltpData("NFO", ce.symbol, ce.token), retries=2)["data"]["ltp"]
                            )
                        except Exception:
                            ltp = 0

                        status_msg += (
                            f"CE ACTIVE\n"
                            f"Symbol: {ce.symbol}\n"
                            f"Entry: {ce.entry:.2f}\n"
                            f"LTP: {ltp:.2f}\n"
                            f"Target: {ce.target:.2f}\n"
                            f"SL: {ce.sl:.2f}\n\n"
                        )
                    else:
                        status_msg += "CE: No Active Trade\n\n"

                    # PE Status
                    if pe and pe.in_position:
                        try:
                            ltp = float(
                                _api_call(lambda: obj.ltpData("NFO", pe.symbol, pe.token), retries=2)["data"]["ltp"]
                            )
                        except Exception:
                            ltp = 0

                        status_msg += (
                            f"PE ACTIVE\n"
                            f"Symbol: {pe.symbol}\n"
                            f"Entry: {pe.entry:.2f}\n"
                            f"LTP: {ltp:.2f}\n"
                            f"Target: {pe.target:.2f}\n"
                            f"SL: {pe.sl:.2f}\n\n"
                        )
                    else:
                        status_msg += "PE: No Active Trade\n\n"

                    # Stats
                    with STATS_LOCK:
                        total = STATS["total_trades"]
                        gp = STATS["gross_profit"]
                        gl = STATS["gross_loss"]

                    net = gp - gl

                    status_msg += (
                        f"Trades Today: {total}\n"
                        f"Net PnL: ₹{net:.0f}\n"
                    )

                    requests.post(
                        f"{base}/sendMessage",
                        data={"chat_id": chat_id, "text": status_msg},
                        timeout=10
                    )

        except Exception:
            time.sleep(3)
# ===== Scrip-master loader & robust LTP =====
def _load_scrip_master(force: bool = False, ttl_seconds: int = 300):
    global _scrip_master_cache, _scrip_master_last_load
    with _scrip_master_lock:
        now = time.time()
        if _scrip_master_cache is None or force or (_scrip_master_last_load and (now - _scrip_master_last_load) > ttl_seconds):
            try:
                r = requests.get(_SCRIP_MASTER_URL, timeout=8)
                j = r.json()
                df = pd.DataFrame(j)
                _scrip_master_cache = df
                _scrip_master_last_load = now
            except Exception as e:
                print(f"[scrip_master] load failed: {e}")
                if _scrip_master_cache is None:
                    _scrip_master_cache = pd.DataFrame()
    return _scrip_master_cache

def robust_get_ltp(exchange: str, tradingsymbol: str = None, symboltoken: str = None, tries: int = 2):
    last_exc = None
    for attempt in range(tries):
        try:
            if symboltoken:
                resp = _api_call(lambda: obj.ltpData(exchange, tradingsymbol or "", str(symboltoken)), retries=2)
            else:
                resp = _api_call(lambda: obj.ltpData(exchange, tradingsymbol, ""), retries=2)
            if resp and isinstance(resp, dict) and "data" in resp and resp["data"]:
                d = resp["data"]
                if isinstance(d, dict) and "ltp" in d:
                    return float(d["ltp"])
                if isinstance(d, list) and len(d) > 0 and isinstance(d[0], dict) and "ltp" in d[0]:
                    return float(d[0]["ltp"])
            last_exc = Exception(f"Invalid LTP response: {resp}")
        except Exception as e:
            last_exc = e
            msg = str(e).lower()
            if ("failed to get symbol details" in msg) or ("ab1018" in msg) or ("symbol details" in msg) or ("invalid token" in msg) or ("no symbol found" in msg):
                try:
                    df = _load_scrip_master()
                    if df is None or df.empty:
                        df = _load_scrip_master(force=True)
                    if isinstance(df, pd.DataFrame) and not df.empty and tradingsymbol:
                        row = df[(df["symbol"]==tradingsymbol) & (df["exch_seg"].str.contains("NFO", na=False))]
                        if row.empty:
                            row = df[(df["name"]==tradingsymbol) & (df["exch_seg"].str.contains("NFO", na=False))]
                        if not row.empty:
                            resolved_tok = str(row.iloc[0]["token"])
                            resolved_sym = row.iloc[0]["symbol"]
                            try:
                                resp2 = _api_call(lambda: obj.ltpData(exchange, resolved_sym, resolved_tok), retries=2)
                                if resp2 and "data" in resp2 and resp2["data"]:
                                    d2 = resp2["data"]
                                    if isinstance(d2, dict) and "ltp" in d2:
                                        return float(d2["ltp"])
                                    if isinstance(d2, list) and len(d2)>0 and "ltp" in d2[0]:
                                        return float(d2[0]["ltp"])
                            except Exception as e2:
                                last_exc = e2
                except Exception as se:
                    last_exc = se
            if "session" in msg or "expired" in msg:
                try:
                    _create_session()
                except Exception:
                    pass
            time.sleep(0.5 * (attempt + 1))
            continue
    raise last_exc

# ===== ATM token fetch using robust_get_ltp =====
def fetch_atm_option_tokens():
    """
    Robust ATM option selector (returns ce_symbol, ce_token, pe_symbol, pe_token, expiry_str)
    - Uses scrip-master JSON (cached by caller if you add caching) and broker LTP lookups.
    - Prefers ATM strike; falls back to nearby strikes if ATM LTP < MIN_OPTION_PREMIUM.
    - Tries NFO then NSE for spot LTP resolution.
    """
    # configurable thresholds (re-use globals if present)
    MIN_OPTION_PREMIUM_LOCAL = globals().get("MIN_OPTION_PREMIUM", 8.0)
    VOL_LOOKBACK_LOCAL = globals().get("VOL_LOOKBACK", 6)
    VOL_MIN_MULT_LOCAL = globals().get("VOL_MIN_MULT", 0.65)

    # fetch scrip master
    try:
        sm = _load_scrip_master(ttl_seconds=600)  # cache for 5 minutes
        if sm is None or sm.empty:
            raise RuntimeError("Empty scrip master cache")
    except Exception as e:
        raise RuntimeError(f"[fetch_atm_option_tokens] could not load cached scrip master: {e}")

    # filter for option rows of our symbol
    opt_df = sm[(sm.get("name") == SPOT_SYMBOL) & (sm.get("exch_seg") == "NFO") & (sm.get("instrumenttype") == "OPTIDX")].copy()
    if opt_df.empty:
        raise RuntimeError("[fetch_atm_option_tokens] no OPTIDX rows for symbol in scrip master")

    # parse expiry safely (use .loc to avoid SettingWithCopyWarning)
    opt_df.loc[:, "expiry_parsed"] = pd.to_datetime(opt_df.get("expiry", pd.Series()), format="%d%b%Y", errors="coerce")
    opt_df = opt_df.dropna(subset=["expiry_parsed"]).copy()
    if opt_df.empty:
        raise RuntimeError("[fetch_atm_option_tokens] no parsed expiry rows in scrip master")

    today = datetime.now().date()
    future_df = opt_df[opt_df["expiry_parsed"].dt.date >= today].copy()
    if future_df.empty:
        # fall back to earliest expiry if none >= today
        future_df = opt_df.sort_values("expiry_parsed").groupby("symbol").first().reset_index()
        if future_df.empty:
            raise RuntimeError("[fetch_atm_option_tokens] no future expiries in scrip master")

    # get spot LTP (try NFO then NSE)
    try:
        spot_ltp = None
        try:
            spot_resp = _api_call(lambda: obj.ltpData("NFO", SPOT_SYMBOL, SPOT_TOKEN), retries=2)
            spot_ltp = float(spot_resp["data"]["ltp"])
        except Exception:
            spot_resp = _api_call(lambda: obj.ltpData("NSE", SPOT_SYMBOL, SPOT_TOKEN), retries=2)
            spot_ltp = float(spot_resp["data"]["ltp"])
    except Exception as e:
        # fallback: try reading from scrip master if it contains a lastPrice-like column (rare)
        spot_ltp = None

    if not spot_ltp or spot_ltp <= 0:
        # fallback to nearest FUTIDX row LTP attempt (less ideal)
        fut_rows = sm[(sm.get("name") == SPOT_SYMBOL) & (sm.get("instrumenttype") == "FUTIDX")].copy()
        if not fut_rows.empty:
            try:
                fut_tok = str(fut_rows.iloc[0]["token"])
                fut_sym = fut_rows.iloc[0]["symbol"]
                fut_resp = _api_call(lambda: obj.ltpData("NFO", fut_sym, fut_tok), retries=2)
                spot_ltp = float(fut_resp["data"]["ltp"])
            except Exception:
                pass

    if not spot_ltp or spot_ltp <= 0:
        raise RuntimeError("[fetch_atm_option_tokens] could not determine spot LTP (needed to pick ATM strike)")

    strike = int(round(float(spot_ltp) / 100.0)) * 100

    # helper to validate a candidate row
    def _validate_option_row(row):
        try:
            sym = row["symbol"]
            tok = str(row["token"])
            # try getting LTP for this option
            try:
                l = _api_call(lambda: obj.ltpData("NFO", sym, tok), retries=2)
                ltp = float(l["data"]["ltp"])
            except Exception:
                return False, 0.0
            if ltp < MIN_OPTION_PREMIUM_LOCAL:
                return False, ltp
            # try getting recent volume via candles
            try:
                cdf = fetch_candle_data(tok)
                vol = int(cdf["volume"].iloc[-1]) if cdf is not None and len(cdf) else 0
            except Exception:
                vol = 0
            return True, ltp if vol >= 0 else ltp
        except Exception:
            return False, 0.0

    # search offsets (prefer ATM then nearby)
    offsets = [0, -100, 100, -200, 200, -300, 300]
    chosen_ce = None
    chosen_pe = None
    chosen_expiry = None

    # restrict to expiries sorted ascending
    future_df = future_df.sort_values("expiry_parsed")

    for o in offsets:
        s = strike + o
        ce_rows = future_df[future_df["symbol"].str.endswith(f"{s}CE")]
        pe_rows = future_df[future_df["symbol"].str.endswith(f"{s}PE")]

        if chosen_ce is None and not ce_rows.empty:
            cand = ce_rows.iloc[0]
            ok, ltp = _validate_option_row(cand)
            if ok:
                chosen_ce = (cand["symbol"], str(cand["token"]))
                chosen_expiry = str(cand["expiry_parsed"].date())
        if chosen_pe is None and not pe_rows.empty:
            cand = pe_rows.iloc[0]
            ok, ltp = _validate_option_row(cand)
            if ok:
                chosen_pe = (cand["symbol"], str(cand["token"]))
                if chosen_expiry is None:
                    chosen_expiry = str(cand["expiry_parsed"].date())

        if chosen_ce and chosen_pe:
            break

    # final fallback: take first available CE/PE for earliest expiry
    if not (chosen_ce and chosen_pe):
        try:
            ce_rows = future_df[future_df["symbol"].str.endswith("CE")].sort_values("expiry_parsed")
            pe_rows = future_df[future_df["symbol"].str.endswith("PE")].sort_values("expiry_parsed")
            if not ce_rows.empty and not pe_rows.empty:
                ce = ce_rows.iloc[0]
                pe = pe_rows.iloc[0]
                chosen_ce = (ce["symbol"], str(ce["token"]))
                chosen_pe = (pe["symbol"], str(pe["token"]))
                chosen_expiry = str(ce["expiry_parsed"].date())
        except Exception:
            pass

    if not (chosen_ce and chosen_pe):
        raise RuntimeError("[fetch_atm_option_tokens] Could not select CE/PE tokens (no suitable rows).")

    return chosen_ce[0], chosen_ce[1], chosen_pe[0], chosen_pe[1], chosen_expiry

# ===== Candle fetch (uses broker getCandleData) =====
def fetch_candle_data(token):
    now = datetime.now()

    # Market official start
    market_start = now.replace(hour=9, minute=15, second=0, microsecond=0)

    # 🔒 If before 9:15 → do nothing safely
    if now < market_start:
        return None

    # 🔒 If less than 3 minutes after open, not enough candles
    if now < market_start + timedelta(minutes=3):
        return None

    # 🔥 Use LAST COMPLETED 3-min candle (correct alignment)
    minute_block = (now.minute // 3) * 3
    safe_to_time = now.replace(minute=minute_block, second=0, microsecond=0)

    # If alignment points to current forming candle, step back one candle
    if safe_to_time >= now:
        safe_to_time -= timedelta(minutes=3)

    params = {
        "exchange": "NFO",
        "symboltoken": str(token),
        "interval": "THREE_MINUTE",
        "fromdate": market_start.strftime("%Y-%m-%d %H:%M"),
        "todate": safe_to_time.strftime("%Y-%m-%d %H:%M"),
    }

    try:
        res = _api_call(lambda: obj.getCandleData(params), retries=3)
    except Exception:
        return None

    if not res or "data" not in res or not res["data"]:
        return None

    df = pd.DataFrame(
        res["data"],
        columns=["timestamp", "open", "high", "low", "close", "volume"]
    )

    df[["open", "high", "low", "close"]] = df[
        ["open", "high", "low", "close"]
    ].astype(float)

    return df

# ===== Order confirmation helpers (MARKET buy + confirm; SELL + confirm) =====
def _extract_net_position_from_positions(resp, symbol_token):
    try:
        rows = resp.get("data", [])
        if isinstance(rows, dict) and "netPositions" in rows:
            rows = rows["netPositions"]
        for r in rows or []:
            tok = str(r.get("symboltoken") or r.get("token") or "")
            if tok == str(symbol_token):
                q = (r.get("netqty") or r.get("netQty") or r.get("net_quantity") or 0)
                return max(0, int(float(q)))
    except Exception:
        pass
    return 0

def _avg_buy_price_from_tradebook(tb, token_str):
    try:
        rows = tb.get("data", []) if isinstance(tb, dict) else (tb if isinstance(tb, list) else [])
        total_qty = 0; total_amt = 0.0
        for t in rows or []:
            tok = str(t.get("symboltoken") or t.get("token") or "")
            if tok != str(token_str): continue
            side = (t.get("transactiontype") or t.get("tradetype") or "").upper()
            qty = int(float(t.get("quantity") or 0))
            price = float(t.get("price") or t.get("tradeprice") or 0.0)
            if side == "BUY" and qty > 0:
                total_qty += qty; total_amt += qty * price
        if total_qty > 0:
            return float(total_amt / total_qty)
    except Exception:
        pass
    return 0.0

def _avg_sell_price_from_tradebook(tb, token_str):
    try:
        rows = tb.get("data", []) if isinstance(tb, dict) else (tb if isinstance(tb, list) else [])
        total_qty = 0
        total_amt = 0.0

        for t in rows or []:
            tok = str(t.get("symboltoken") or t.get("token") or "")
            if tok != str(token_str):
                continue

            side = (t.get("transactiontype") or t.get("tradetype") or "").upper()
            qty = int(float(t.get("quantity") or 0))
            price = float(t.get("price") or t.get("tradeprice") or 0.0)

            if side == "SELL" and qty > 0:
                total_qty += qty
                total_amt += qty * price

        if total_qty > 0:
            return float(total_amt / total_qty)

    except Exception:
        pass

    return 0.0

def _net_exec_qty_from_tradebook(tb, token_str):
    try:
        rows = tb.get("data", []) if isinstance(tb, dict) else (tb if isinstance(tb, list) else [])
        net = 0
        for t in rows or []:
            tok = str(t.get("symboltoken") or t.get("token") or "")
            if tok != str(token_str): continue
            side = (t.get("transactiontype") or t.get("tradetype") or "").upper()
            qty  = int(float(t.get("quantity") or 0))
            if side == "BUY": net += qty
            elif side == "SELL": net -= qty
        return max(0, net)
    except Exception:
        return 0

def _get_open_qty(symbol_token):
    try:
        pos = _api_call(lambda: obj.position(), retries=3)
        return _extract_net_position_from_positions(pos, symbol_token)
    except Exception as e:
        print(f"[Position Check] position() API failed: {e}")
        return None  # IMPORTANT: return None instead of 0

def _persist_state():
    try:
        os.makedirs(os.path.dirname(STATE_PERSIST_FILE) or ".", exist_ok=True)
        with open(STATE_PERSIST_FILE, "w") as fh:
            json.dump({"baselines": BOT_BASELINES, "avg": BOT_AVG_ENTRY}, fh)
    except Exception as e:
        print(f"[state] persist failed: {e}")

def _load_state():
    global BOT_BASELINES, BOT_AVG_ENTRY
    try:
        if os.path.exists(STATE_PERSIST_FILE):
            with open(STATE_PERSIST_FILE, "r") as fh:
                j = json.load(fh)
                BOT_BASELINES = {str(k): int(v) for k,v in j.get("baselines", {}).items()}
                BOT_AVG_ENTRY = {str(k): float(v) for k,v in j.get("avg", {}).items()}
                print(f"[state] loaded persisted state baselines={len(BOT_BASELINES)} avg_entries={len(BOT_AVG_ENTRY)}")
    except Exception as e:
        print(f"[state] load failed: {e}")

def _invalidate_and_wait(token, delay=0.25):
    time.sleep(delay)

def place_market_and_confirm_buy(symbol, token, total_qty, side_lock_name=None,
                                 confirm_timeout=30, poll_sec=1.0, persist_on_confirm=True):
    placed_resp = None
    try:
        payload = {
            "variety": "NORMAL",
            "tradingsymbol": symbol,
            "symboltoken": str(token),
            "transactiontype": "BUY",
            "exchange": "NFO",
            "ordertype": "MARKET",
            "producttype": "INTRADAY",
            "duration": "DAY",
            "price": "0",
            "quantity": int(total_qty)
        }
        placed_resp = _api_call(lambda: obj.placeOrder(payload), retries=3)
        time.sleep(0.2)
    except Exception as e:
        if side_lock_name:
            try:
                with DAY_STATE_LOCK:
                    global TRADING_ENGINE_ACTIVE
                    TRADING_ENGINE_ACTIVE = False
            except Exception:
                pass
        print(f"[Order] placeOrder failed: {e}")
        return {"ok": False, "placed_resp": None, "order_id": None, "filled_qty": 0, "avg_price": None, "debug": {"place_exception": str(e)}}

    try:
        _invalidate_and_wait(token, delay=0.25)
    except Exception:
        pass

    order_id = None
    try:
        if isinstance(placed_resp, dict):
            d = placed_resp.get("data") if "data" in placed_resp else placed_resp
            if isinstance(d, dict):
                for k in ("orderid","orderNo","orderId","uniqueOrderID"):
                    if k in d:
                        order_id = d.get(k); break
            else:
                if isinstance(placed_resp, str):
                    order_id = placed_resp
    except Exception:
        pass

    start = time.time()
    filled_qty = 0
    avg_price = None
    debug = {"placed_resp": placed_resp, "positions": None, "tradeBook": None, "orderBook": None}
    timeout = float(confirm_timeout)

    while time.time() - start < timeout:
        try:
            try:
                pos = _api_call(lambda: obj.position(), retries=2)
            except Exception:
                pos = None
            debug["positions"] = pos
            if pos:
                q = _extract_net_position_from_positions(pos, token)
                if q and q > 0:
                    filled_qty = max(filled_qty, q)
                    try:
                        rows = pos.get("data", [])
                        if isinstance(rows, dict) and "netPositions" in rows:
                            rows = rows["netPositions"]
                        for r in rows or []:
                            tok = str(r.get("symboltoken") or r.get("token") or "")
                            if tok == str(token):
                                avg_price = float(r.get("avgPrice") or r.get("avg_price") or avg_price or 0.0)
                                break
                    except Exception:
                        pass
                    if filled_qty > 0:
                        break

            try:
                tb = _api_call(lambda: obj.tradeBook(), retries=2)
            except Exception:
                tb = None
            debug["tradeBook"] = tb
            if tb:
                tb_qty = _net_exec_qty_from_tradebook(tb, token)
                if tb_qty > 0:
                    filled_qty = max(filled_qty, tb_qty)
                    avg_price = avg_price or _avg_buy_price_from_tradebook(tb, token) or avg_price
                    if filled_qty > 0:
                        break

            try:
                ob = None
                if hasattr(obj, "orderBook"):
                    try:
                        ob = _api_call(lambda: obj.orderBook(), retries=1)
                    except Exception:
                        ob = None
                debug["orderBook"] = ob
                if ob:
                    rows = ob.get("data") if isinstance(ob, dict) and "data" in ob else (ob if isinstance(ob, list) else [])
                    for o in rows or []:
                        o_tok = str(o.get("symboltoken") or o.get("token") or "")
                        if o_tok == str(token) or (order_id and (str(order_id) in str(o.get("orderNo") or o.get("orderId") or ""))):
                            status = (o.get("status") or o.get("orderstatus") or "").lower()
                            text = str(o.get("text") or "")
                            if "reject" in status or "rejected" in status or "insufficient" in text.lower():
                                if side_lock_name:
                                    try:
                                        with DAY_STATE_LOCK:
                                            TRADING_ENGINE_ACTIVE = False
                                    except Exception:
                                        pass
                                return {"ok": False, "placed_resp": placed_resp, "order_id": order_id, "filled_qty": 0, "avg_price": None, "debug": debug}
            except Exception:
                pass

        except Exception as e:
            print(f"[confirm] poll error: {e}")
        time.sleep(poll_sec)

    # final checks
    if filled_qty <= 0:
        try:
            pos = _api_call(lambda: obj.position(), retries=2)
            debug["positions_after"] = pos
            q = _extract_net_position_from_positions(pos, token)
            if q and q > 0:
                filled_qty = max(filled_qty, q)
        except Exception:
            pass
        try:
            tb = _api_call(lambda: obj.tradeBook(), retries=2)
            debug["tradeBook_after"] = tb
            tb_qty = _net_exec_qty_from_tradebook(tb, token)
            if tb_qty > 0:
                filled_qty = max(filled_qty, tb_qty)
                avg_price = avg_price or _avg_buy_price_from_tradebook(tb, token) or avg_price
        except Exception:
            pass

    ok = filled_qty > 0
    if ok:
        avg_price = float(avg_price or 0.0)
        try:
            prev_baseline = int(BOT_BASELINES.get(str(token), 0))
            BOT_BASELINES[str(token)] = prev_baseline
            BOT_AVG_ENTRY[str(token)] = float(avg_price)
            if persist_on_confirm:
                _persist_state()
        except Exception:
            pass
        return {"ok": True, "placed_resp": placed_resp, "order_id": order_id, "filled_qty": int(filled_qty), "avg_price": float(avg_price), "debug": debug}
    else:
        if side_lock_name:
            try:
                with DAY_STATE_LOCK:
                    TRADING_ENGINE_ACTIVE = False
            except Exception:
                pass
        try:
            _tg_send(f"⚠️ ENTRY UNCONFIRMED {symbol}\nNo fills observed within {confirm_timeout}s. Order resp: {str(placed_resp)[:240]}")
        except Exception:
            pass
        return {"ok": False, "placed_resp": placed_resp, "order_id": order_id, "filled_qty": 0, "avg_price": None, "debug": debug}

def place_market_and_confirm_sell(symbol, token, qty_to_sell, timeout_sec=25, poll_sec=1.0):
    try:
        pre_open = _get_open_qty(token)
    except Exception:
        pre_open = None
    payload = {
        "variety":"NORMAL","tradingsymbol": symbol,"symboltoken": str(token),
        "transactiontype":"SELL","exchange":"NFO","ordertype":"MARKET",
        "producttype":"INTRADAY","duration":"DAY","price":"0","quantity": int(qty_to_sell)
    }
    try:
        resp = _api_call(lambda: obj.placeOrder(payload), retries=3)
        time.sleep(0.2)
    except Exception as e:
        print(f"[place_sell] placeOrder failed: {e}")
        _tg_send(f"❌ SELL place error: {e}")
        return 0
    start = time.time()
    prev = pre_open if pre_open is not None else _get_open_qty(token)
    try:
        pre_tb_net = _net_exec_qty_from_tradebook(_api_call(lambda: obj.tradeBook(), retries=2), token)
    except Exception:
        pre_tb_net = 0
    while time.time() - start < float(timeout_sec):
        try:
            open_qty = _get_open_qty(token)
            try:
                tb = _api_call(lambda: obj.tradeBook(), retries=2)
                tb_net = _net_exec_qty_from_tradebook(tb, token)
                sold_by_tb = max(0, (pre_tb_net or 0) - (tb_net or 0))
                if sold_by_tb > 0:
                    sold = min(int(sold_by_tb), int(qty_to_sell))
                    _invalidate_and_wait(token)
                    return sold
            except Exception:
                pass
            if prev is None:
                prev = open_qty
            else:
                if open_qty < prev:
                    sold = max(0, prev - open_qty)
                    sold = min(int(sold), int(qty_to_sell))
                    _invalidate_and_wait(token)
                    return sold
        except Exception as e:
            print(f"[place_sell] poll error: {e}")
        time.sleep(poll_sec)
    try:
        cur = _get_open_qty(token)
        if prev is not None and cur < prev:
            sold = max(0, prev - cur)
            sold = min(int(sold), int(qty_to_sell))
            _invalidate_and_wait(token)
            return sold
    except Exception:
        pass
    try:
        tb = _api_call(lambda: obj.tradeBook(), retries=2)
        tb_net_after = _net_exec_qty_from_tradebook(tb, token)
        sold_by_tb_final = max(0, pre_tb_net - tb_net_after)
        if sold_by_tb_final > 0:
            sold = min(int(sold_by_tb_final), int(qty_to_sell))
            _invalidate_and_wait(token)
            return sold
    except Exception:
        pass
    return 0

ATM_CACHE = {"ts": 0, "data": None}

def fetch_atm_cached(ttl=60):
    now = time.time()
    if ATM_CACHE["data"] and (now - ATM_CACHE["ts"] < ttl):
        return ATM_CACHE["data"]
    data = fetch_atm_option_tokens()
    ATM_CACHE["data"] = data
    ATM_CACHE["ts"] = now
    return data


# ===== RedGreen Engine (per leg) =====
class RedGreenEngine(threading.Thread):
    def __init__(self, name: str, pick_symbol_fn=None):
        super().__init__(daemon=True)
        self.name = name
        self.pick_symbol_fn = pick_symbol_fn
        self.in_position = False
        self.entry = None
        self.target = None
        self.sl = None
        self.symbol = None
        self.token = None
        self.expiry = None
        self.entry_volume = 0
        self.position_qty = 0
        self._exit_request = None
        self._exit_req_lock = threading.Lock()
        self._exiting = False
        self._current_order_id = None  # keep order id for cancellations if needed

    def _sleep_until_next_3min(self):
        now = datetime.now()
        candle_start_min = (now.minute // 3) * 3
        candle_start = now.replace(minute=candle_start_min, second=0, microsecond=0)
        next_candle_time = candle_start + timedelta(minutes=3)
        wait_seconds = max(0, int((next_candle_time - now).total_seconds()))
        print(f"[{self.name}] ⏳ Sleeping until next 3-min candle at {next_candle_time.strftime('%H:%M:%S')} ({wait_seconds} sec)...")
        for remaining in range(wait_seconds, 0, -1):
            if not PROGRAM_RUNNING:
                return
            if _market_closed():
                print("")
                break
            print(f"[{self.name}] 🕒 Sleeping: {remaining} sec...", end="\r", flush=True)
            time.sleep(1)
        print("")

    def _other_leg_in_position(self) -> bool:
        """
        Return True if *any* other engine (the opposite leg) currently holds a position.
        Uses the global ENGINES registry (populated in __main__).
        """
        try:
            for name, eng in ENGINES.items():
                if name == self.name:
                    continue
                if getattr(eng, "in_position", False):
                    return True
        except Exception:
            # conservative: assume other leg may be in position on error
            return True
        return False

    def _detect_and_enter(self, df):
        global TRADING_ENGINE_ACTIVE
        # global DAY_HAS_TRADE

        # Basic safety
        if df is None or len(df) < 6:
            return False

        prev = df.iloc[-3]
        curr = df.iloc[-2]

        # --- Red-Green Condition ---
        if not (prev["close"] < prev["open"] and curr["close"] > curr["open"]):
            return False

        # ============================
        # BALANCED CANDLE STRUCTURE
        # ============================

        body = abs(curr["close"] - curr["open"])
        range_size = curr["high"] - curr["low"]

        lower_wick = min(curr["open"], curr["close"]) - curr["low"]
        upper_wick = curr["high"] - max(curr["open"], curr["close"])

        if range_size < 5:
            return False

        if upper_wick > body:
            return False

        if not (body > range_size * 0.5 or lower_wick > body * 1.5):
            return False

        print(
            f"[{self.name}] Structure → "
            f"Body: {body:.2f} | Range: {range_size:.2f} | "
            f"LowerWick: {lower_wick:.2f} | UpperWick: {upper_wick:.2f}"
        )

        baseline_vol = df["volume"].iloc[-6:-2].mean()

        print(
            f"[{self.name}] Volume Check → "
            f"Prev: {prev['volume']} | "
            f"Curr: {curr['volume']} | "
            f"Baseline Avg: {baseline_vol:.0f}"
        )

        if not (
            curr["volume"] > prev["volume"] or
            curr["volume"] > baseline_vol * 1.05
        ):
            print(f"[{self.name}] ❌ Volume condition FAILED")
            return False

        print(f"[{self.name}] ✅ Volume condition PASSED")

        if not RUN_FLAG or not _entry_window_open() or _market_closed():
            return False

        # Ensure token already available (avoid redundant API calls)
        if not self.symbol or not self.token:
            print(f"[{self.name}] No token available for entry.")
            return False

        # =============================
        # ENTRY LOCK (SAFE VERSION)
        # =============================
        acquired = False
        with DAY_STATE_LOCK:
            if not TRADING_ENGINE_ACTIVE:
                TRADING_ENGINE_ACTIVE = True
                acquired = True

        if not acquired:
            return False

        try:
            print(f"[Order] placing MARKET BUY qty={LOT_SIZE} symbol={self.symbol} token={self.token}")

            res = place_market_and_confirm_buy(
                self.symbol,
                self.token,
                LOT_SIZE,
                side_lock_name=self.name,
                confirm_timeout=30,
                poll_sec=1.0,
                persist_on_confirm=True
            )

            print(f"[{self.name}] BUY order result: {res}")
            self._current_order_id = res.get("order_id")

            if not res or not res.get("ok", False):
                print(f"[{self.name}] No fill received for BUY; not entering position.")
                return False

            filled = int(res.get("filled_qty", 0))
            avg = res.get("avg_price", None)

            if not avg or avg <= 0:
                try:
                    avg = float(
                        _api_call(lambda: obj.ltpData("NFO", self.symbol, self.token), retries=2)["data"]["ltp"]
                    )
                except Exception:
                    avg = float(curr["close"])

            self.position_qty = filled if filled > 0 else LOT_SIZE
            self.entry = float(avg)
            self.target = round(self.entry + TARGET_POINTS, 2)
            self.sl = round(self.entry - SL_POINTS, 2)
            self.entry_volume = int(curr["volume"]) if "volume" in curr.index else 0
            self.in_position = True
            self._exiting = False

            print(
                f"[{self.name}] ✅ Entered @ {self.entry} | "
                f"Qty {self.position_qty} | "
                f"Tgt {self.target} | SL {self.sl} | Symbol {self.symbol}"
            )

            return True

        except Exception as e:
            print(f"[{self.name}] ENTRY EXCEPTION: {e}")
            return False

        finally:
            # ALWAYS RELEASE ENTRY LOCK
            with DAY_STATE_LOCK:
                TRADING_ENGINE_ACTIVE = False
    

    def _exit_and_log(self, exit_price, reason):
        qty = self.position_qty if self.position_qty > 0 else LOT_SIZE

        # -------------------------------------------------
        # 1️⃣ Try broker REALISED PnL first
        # -------------------------------------------------
        net_pnl = None

        try:
            pos = _api_call(lambda: obj.position(), retries=2)

            rows = pos.get("data", []) if isinstance(pos, dict) else []

            if isinstance(rows, dict) and "netPositions" in rows:
                rows = rows["netPositions"]

            for r in rows:
                tok = str(r.get("symboltoken") or r.get("token") or "")
                if tok == str(self.token):

                    realised = (
                        r.get("realisedpnl")
                        or r.get("realisedPnl")
                        or r.get("pnl")
                        or 0
                    )

                    try:
                        net_pnl = float(realised)
                    except Exception:
                        net_pnl = None

                    break

        except Exception as e:
            print(f"[{self.name}] position() PnL fetch failed: {e}")

        # -------------------------------------------------
        # 2️⃣ Fallback to manual calculation
        # -------------------------------------------------
        if net_pnl is None:
            gross_pnl = (exit_price - self.entry) * qty
            net_pnl = gross_pnl - BROKERAGE_TAX

        # -------------------------------------------------
        # 3️⃣ Log trade
        # -------------------------------------------------
        try:
            log_trade(
                self.symbol,
                "BUY",
                "Red-Green",
                trigger_price=self.entry,
                entry_price=self.entry,
                target_price=self.target,
                sl_price=self.sl,
                exit_price=exit_price,
                result=reason,
                pnl=net_pnl,
                volume=self.entry_volume,
                expiry=self.expiry
            )
        except Exception as e:
            print(f"[{self.name}] log_trade failed (continuing): {e}")

        # -------------------------------------------------
        # 4️⃣ Stats + Chart + Telegram
        # -------------------------------------------------
        try:
            _update_stats_with_pnl(net_pnl)
            print_trade_summary()

            # ==============================
            # UPDATED CHART SENDING BLOCK
            # ==============================
            try:
                png = "RAHUL/Rahul_daily_pnls.png"

                chart_ok = _build_daily_pnl_chart(TRADE_LOG_FILE, png)

                if chart_ok and os.path.exists(png):
                    print("[Chart] Built successfully.")
                    print("[Chart] Waiting 5 seconds before sending to Telegram...")

                    time.sleep(5)

                    sent = _tg_send_photo(png, caption="📊 Updated Daily PnL Chart")

                    if sent:
                        print("[Chart] Telegram photo sent successfully.")
                    else:
                        print("[Chart] Telegram photo send failed.")
                else:
                    print("[Chart] Build failed or PNG missing.")

            except Exception as e:
                print(f"[Chart Error] {e}")
            # ==============================

            try:
                with STATS_LOCK:
                    t  = STATS["total_trades"]
                    w  = STATS["profit_trades"]
                    l  = STATS["loss_trades"]
                    gp = STATS["gross_profit"]
                    gl = STATS["gross_loss"]
            except Exception:
                t = w = l = 0
                gp = gl = 0.0

            try:
                msg = (
                    f"📣 {self.name} {reason}\n"
                    f"Symbol: {self.symbol}\n"
                    f"Qty: {qty}\n"
                    f"Entry: {self.entry:.2f} → Exit: {exit_price:.2f}\n"
                    f"PnL (net): ₹{net_pnl:.0f}\n\n"
                    f"📊 Totals — Trades: {t} | Win: {w} | Loss: {l}\n"
                    f"Profit: ₹{gp:.0f} | Loss: ₹{gl:.0f}"
                )

                _tg_send(msg)

            except Exception:
                pass

        except Exception as e:
            print(f"[{self.name}] stats/notify failed (continuing): {e}")

    def _sell_and_exit(self, reason: str) -> bool:
        if self._exiting:
            print(f"[{self.name}] Exit already in progress; skipping duplicate SELL.")
            return False
        self._exiting = True

        try:
            qty = self.position_qty or LOT_SIZE
            print(f"[{self.name}] placing MARKET SELL qty={qty} symbol={self.symbol} token={self.token}")
            sold_qty = place_market_and_confirm_sell(self.symbol, self.token, qty, timeout_sec=25, poll_sec=1.0)
            if sold_qty <= 0:
                print(f"[{self.name}] ⚠ SELL not confirmed — position may still be open!")
            # Get actual SELL average price from tradeBook
            exit_px = self.entry  # fallback safety

            try:
                try:
                    tb = _api_call(lambda: obj.tradeBook(), retries=2)
                    avg_sell = _avg_sell_price_from_tradebook(tb, self.token)
                except Exception:
                    avg_sell = None

                if avg_sell and avg_sell > 0:
                    exit_px = float(avg_sell)
                else:
                    print(f"[{self.name}] WARNING: Could not extract sell avg from tradeBook. Using LTP fallback.")
                    try:
                        exit_px = float(
                            _api_call(lambda: obj.ltpData("NFO", self.symbol, self.token), retries=2)["data"]["ltp"]
                        )
                    except Exception:
                        exit_px = self.entry

            except Exception as e:
                print(f"[{self.name}] tradeBook fetch failed: {e}")

            self._exit_and_log(exit_px, reason)
            return True
        finally:
            self.in_position = False
            self.position_qty = 0
            # Mark trading as finished (DAY_HAS_TRADE already set on entry) and clear active flag
            with DAY_STATE_LOCK:
                global TRADING_ENGINE_ACTIVE
                TRADING_ENGINE_ACTIVE = False
                # Clear persisted baseline so bot doesn't think it's still baseline (optional)
                try:
                    BOT_BASELINES.pop(str(self.token), None)
                    BOT_AVG_ENTRY.pop(str(self.token), None)
                    _persist_state()
                except Exception:
                    pass

    def run(self):
        print(f"[{self.name}] Engine started.")

        while PROGRAM_RUNNING:
            try:
                # ============================
                # Market Close Handling
                # ============================
                if _market_closed():
                    if self.in_position:
                        try:
                            self._sell_and_exit("MKT_CLOSE")
                        except Exception as e:
                            print(f"[{self.name}] error during MKT_CLOSE exit: {e}")
                    time.sleep(2)
                    continue

                # ============================
                # Fetch ATM token
                # ============================
                try:
                    ce_sym, ce_tok, pe_sym, pe_tok, expiry = fetch_atm_cached()

                    if self.name == "CE":
                        self.symbol, self.token, self.expiry = ce_sym, ce_tok, expiry
                    else:
                        self.symbol, self.token, self.expiry = pe_sym, pe_tok, expiry

                except Exception as e:
                    print(f"[{self.name}] token fetch error: {e}")
                    self._sleep_until_next_3min()
                    continue

                # ============================
                # Fetch Candles
                # ============================
                try:
                    df = fetch_candle_data(self.token)
                except Exception as e:
                    print(f"[{self.name}] candle fetch error: {e}")
                    df = None

                if df is None:
                    self._sleep_until_next_3min()
                    continue

                # ============================
                # ENTRY LOGIC
                # ============================
                if not self.in_position and RUN_FLAG and _entry_window_open():

                    if self._other_leg_in_position():
                        print(f"[{self.name}] Skipping entry because opposite leg already in trade.")
                        self._sleep_until_next_3min()
                        continue

                    if _should_block_new_entries():
                        self._sleep_until_next_3min()
                        continue

                    try:
                        entered = self._detect_and_enter(df)
                        if entered:
                            self.in_position = True
                    except Exception as e:
                        print(f"[{self.name}] enter error: {e}")

                # ============================
                # MONITOR TRADE
                # ============================
                while self.in_position and not _market_closed():
                    if API_CIRCUIT_OPEN:
                        print(f"[{self.name}] Circuit open — stopping monitor loop.")
                        break
                    # Manual exit request
                    req = None
                    with self._exit_req_lock:
                        if self._exit_request:
                            req = self._exit_request
                            self._exit_request = None

                    if req:
                        self._sell_and_exit(req)
                        break

                    if not RUN_FLAG:
                        self._sell_and_exit("PAUSE_EXIT")
                        break

                    try:
                        ltp = float(
                            _api_call(lambda: obj.ltpData("NFO", self.symbol, self.token), retries=2)["data"]["ltp"]
                        )

                        print(
                            f"✅[{self.name}] LTP {ltp:.2f} | Qty {self.position_qty} "
                            f"| Tgt {self.target} | SL {self.sl} | Symbol {self.symbol}"
                        )

                        # =====================================
                        # 🔥 AUTO SQUARE-OFF DETECTION
                        # =====================================
                        open_qty = _get_open_qty(self.token)

                        if open_qty is not None and open_qty == 0:
                            print(f"[{self.name}] 🔔 Broker auto square-off detected.")

                            try:
                                tb = _api_call(lambda: obj.tradeBook(), retries=3)
                                avg_sell = _avg_sell_price_from_tradebook(tb, self.token)
                                exit_px = float(avg_sell) if avg_sell else self.entry
                            except Exception:
                                exit_px = self.entry

                            self._exit_and_log(exit_px, "AUTO_SQOFF")
                            self.in_position = False
                            break
                        # =====================================

                        # Normal Exit Conditions
                        if ltp >= self.target:
                            self._sell_and_exit("TARGET")
                            break

                        if ltp <= self.sl:
                            self._sell_and_exit("STOPLOSS")
                            break

                    except Exception as e:
                        print(f"[{self.name}] monitor error: {e}")

                    time.sleep(SLEEP_INTERVAL)

                # ============================
                # Stop for the day if needed
                # ============================
                if _both_legs_flat() and _should_block_new_entries():
                    _stop_for_the_day("(both legs are flat)")

                self._sleep_until_next_3min()

            except Exception as ex_main:
                print(f"[{self.name}] Unexpected error in run loop: {ex_main}")

                with DAY_STATE_LOCK:
                    global TRADING_ENGINE_ACTIVE
                    TRADING_ENGINE_ACTIVE = False

                time.sleep(5)
                continue

# ===== Support functions =====
def _both_legs_flat() -> bool:
    ce = ENGINES.get("CE"); pe = ENGINES.get("PE")
    try:
        return bool(ce and pe and (not ce.in_position) and (not pe.in_position))
    except Exception:
        return False

def _should_block_new_entries() -> bool:
    try:
        # Only respect DAILY_TRADE_LIMIT
        if _compute_today_trade_count_from_csv(TRADE_LOG_FILE) >= DAILY_TRADE_LIMIT:
            return True

        with STATS_LOCK:
            if STATS.get("total_trades", 0) >= DAILY_TRADE_LIMIT:
                return True

        return False

    except Exception:
        return False

def log_trade(option_symbol, direction, signal_type, trigger_price, entry_price,
              target_price, sl_price, exit_price, result, pnl, volume, expiry):
    with FILE_LOCK:
        trade_datetime = datetime.now(); trade_day = trade_datetime.date(); cumulative = 0.0
        try:
            if os.path.exists(TRADE_LOG_FILE):
                with open(TRADE_LOG_FILE, "r", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        row_dt = _parse_row_datetime_safe(row.get("Datetime",""))
                        if row_dt and row_dt.date() == trade_day:
                            try:
                                cumulative += _to_float_safe(row.get("PnL","0"))
                            except Exception:
                                pass
        except Exception:
            pass
        try:
            current_pnl = float(_to_float_safe(pnl))
        except Exception:
            current_pnl = 0.0
        cumulative += current_pnl
        try:
            header = ["Datetime","Option","Direction","Signal Type","Trigger Price","Entry Price",
                      "Target","SL","Exit Price","Result","PnL","Volume","Expiry","Total PnL"]
            write_header = not os.path.exists(TRADE_LOG_FILE) or os.path.getsize(TRADE_LOG_FILE) == 0
            with open(TRADE_LOG_FILE, "a", newline="") as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow(header)
                writer.writerow([
                    trade_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                    option_symbol, direction, signal_type,
                    round(_to_float_safe(trigger_price), 2),
                    round(_to_float_safe(entry_price),   2),
                    round(_to_float_safe(target_price),  2),
                    round(_to_float_safe(sl_price),      2),
                    round(_to_float_safe(exit_price),    2),
                    result, round(current_pnl, 2),
                    int(_to_float_safe(volume)), str(expiry or ""), round(cumulative,2)
                ])
                try:
                    f.flush(); os.fsync(f.fileno())
                except Exception:
                    pass
        except Exception as e:
            print(f"[log_trade] write failed: {e}")
    try:
        update_excel_with_daily_pnl()
    except Exception:
        pass

# ===== Engines registry & start logic =====
ENGINES = {}

if __name__ == "__main__":
    _init_stats_from_csv(TRADE_LOG_FILE)
    _load_state()
    threading.Thread(target=_api_circuit_monitor, daemon=True).start()
    if TELEGRAM_BOT_TOKEN and ALLOWED_CHAT_ID:
        threading.Thread(target=_telegram_listener, daemon=True).start()
    now_t = datetime.now().time()
    if not (_time(00,30) <= now_t <= _time(15,30)):
        print("🛑 Market closed. Stopping strategy execution.")
        RUN_FLAG = False
        time.sleep(2)
        raise SystemExit(0)
    CEEngine = RedGreenEngine("CE")
    PEEngine = RedGreenEngine("PE")
    ENGINES["CE"] = CEEngine; ENGINES["PE"] = PEEngine
    CEEngine.start(); PEEngine.start()
    try:
        _tg_send(f"🟢 Red-Green Bot Started\n"
                f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"SL: {SL_POINTS} | Target: {TARGET_POINTS}\n"
                f"Lot Size: {LOT_SIZE}")
    except Exception:
        pass
    try:
        while PROGRAM_RUNNING:
            now_t = datetime.now().time()
            if _market_closed() or not (_time(00,30) <= now_t <= _time(15,30)):
                print("🛑 Market closed. Stopping strategy execution.")
                RUN_FLAG = False
                time.sleep(3)
                break
            if _both_legs_flat() and _should_block_new_entries():
                print("✅ One-trade gate/limits active and no open positions. Stopping for the day.")
                RUN_FLAG = False; time.sleep(2); break
            with STATS_LOCK:
                if STATS.get("total_trades", 0) >= DAILY_TRADE_LIMIT:
                    print(f"✅ Trade limit ({DAILY_TRADE_LIMIT}) reached; stopping for the day.")
                    _stop_for_the_day(f"(trade limit reached: {STATS.get('total_trades')})")
                    time.sleep(2); break
            time.sleep(5)
    except KeyboardInterrupt:
        print("Interrupted — stopping.")
        RUN_FLAG = False
        time.sleep(1)
