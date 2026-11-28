import json
import os
import random
import time
import threading
import signal
import functools
from datetime import datetime, timedelta, timezone
from curl_cffi import requests as cffi_requests

# =====================================================
# CONFIG — driven by YAML env vars
# =====================================================
SHARD_ID = int(os.environ.get("SHARD_ID", "1"))
if not (1 <= SHARD_ID <= 8):
    raise SystemExit(
        f"Invalid SHARD_ID={SHARD_ID}. daily.py handles shards 1–8 only. "
        f"Use daily9.py for shard 9."
    )

MODE = os.environ.get("MODE", "daily").lower()
if MODE not in ("daily", "advance"):
    raise SystemExit(f"Invalid MODE={MODE}. Use 'daily' or 'advance'.")

CUTOFF_MINUTES = int(os.environ.get("CUTOFF_MINUTES", "210"))

API_TIMEOUT             = 15
HARD_TIMEOUT            = 20
MAX_ATTEMPTS_PER_VENUE  = 5      # initial + 4 retries per venue
IDENTITY_ROTATE_EVERY   = 600    # safety net only
FAILURE_THRESHOLD       = 5      # consecutive venue failures -> cooldown
FAILURE_ROTATE_THRESHOLD = 3     # total attempt failures -> rotate identity
COOLDOWN_SECONDS        = 50
BASE_DELAY              = 1.0
MAX_DELAY               = 2.0
RETRY_BACKOFF_MAX       = 4.0
ROTATE_PAUSE_MIN        = 1.5
ROTATE_PAUSE_MAX        = 3.0

IST = timezone(timedelta(hours=5, minutes=30))

DATE_CODE = os.environ.get("DATE_CODE") or datetime.now(IST).strftime("%Y%m%d")
BASE_DIR  = os.environ.get("BASE_DIR")  or os.path.join("daily", "data", DATE_CODE)

try:
    TARGET_DATE = datetime.strptime(DATE_CODE, "%Y%m%d").replace(tzinfo=IST)
except Exception:
    TARGET_DATE = datetime.now(IST)

LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

DETAILED_FILE = f"{BASE_DIR}/detailed{SHARD_ID}.json"
SUMMARY_FILE  = f"{BASE_DIR}/movie_summary{SHARD_ID}.json"
LOG_FILE      = f"{LOG_DIR}/bms{SHARD_ID}.log"

VENUES_FILE   = f"venues{SHARD_ID}.json"

# =====================================================
# HEADER OPTIONS
#   XFF / X-Real-IP disabled — they don't change source IP,
#   and loopback/reserved values are a strong bot signal.
# =====================================================
USE_XFF       = False
USE_X_REAL_IP = False

EXTRA_HEADERS = {
    "Referer":         "https://in.bookmyshow.com/",
    "Origin":          "https://in.bookmyshow.com",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-IN,en;q=0.9",
}

# =====================================================
# LOGGING
# =====================================================
def log(msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# =====================================================
# HARD TIMEOUT (main-thread only; no-op elsewhere)
# =====================================================
class TimeoutError(Exception):
    pass

def _timeout_handler(signum, frame):
    raise TimeoutError("Hard timeout hit")

def hard_timeout(seconds):
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            if not hasattr(signal, "SIGALRM") or \
               threading.current_thread() is not threading.main_thread():
                return fn(*args, **kwargs)
            signal.signal(signal.SIGALRM, _timeout_handler)
            signal.alarm(seconds)
            try:
                return fn(*args, **kwargs)
            finally:
                signal.alarm(0)
        return wrapper
    return deco

# =====================================================
# IDENTITY
# =====================================================
thread_local    = threading.local()
request_counter = 0

RESERVED_IP_BLOCKS = [
    (0,   0),   (10,  10),  (100, 64),
    (127, 127), (169, 169), (172, 16),
    (172, 31),  (192, 0),   (192, 168),
    (198, 18),  (224, 255),
]

def random_public_ip():
    while True:
        a = random.randint(1, 223)
        if any(lo <= a <= hi for lo, hi in RESERVED_IP_BLOCKS):
            continue
        return (
            f"{a}.{random.randint(0, 255)}"
            f".{random.randint(0, 255)}.{random.randint(1, 254)}"
        )

class Identity:
    def __init__(self):
        self.session = cffi_requests.Session(impersonate="safari")
        self.ip      = random_public_ip()

    def headers(self):
        h = dict(EXTRA_HEADERS)
        if USE_XFF:
            h["X-Forwarded-For"] = self.ip
        if USE_X_REAL_IP:
            h["X-Real-IP"] = self.ip
        return h or None


def get_identity():
    global request_counter
    if not hasattr(thread_local, "identity"):
        thread_local.identity = Identity()
        log(f"🧠 New identity (client_ip={thread_local.identity.ip})")
    request_counter += 1
    if request_counter % IDENTITY_ROTATE_EVERY == 0:
        rotate_identity(f"periodic every {IDENTITY_ROTATE_EVERY}")
    return thread_local.identity


def rotate_identity(reason=""):
    if hasattr(thread_local, "identity"):
        del thread_local.identity
    log(f"🔄 Identity rotated ({reason})")


# =====================================================
# FAILURE COUNTER -> ROTATE
#   Counts every attempt-level failure (initial + retries),
#   across all venues. At threshold, rotate identity.
# =====================================================
_failure_count = 0

def _note_failure():
    global _failure_count
    _failure_count += 1
    if _failure_count >= FAILURE_ROTATE_THRESHOLD:
        log(f"🔁 {_failure_count} failures accumulated – rotating identity")
        rotate_identity("failure threshold")
        _failure_count = 0
        time.sleep(random.uniform(ROTATE_PAUSE_MIN, ROTATE_PAUSE_MAX))

# =====================================================
# FETCH API
# =====================================================
@hard_timeout(HARD_TIMEOUT)
def fetch_api_raw(venue_code):
    ident = get_identity()
    url = (
        "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
        f"?venueCode={venue_code}&dateCode={DATE_CODE}"
    )
    kwargs = dict(timeout=API_TIMEOUT, impersonate="safari")
    headers = ident.headers()
    if headers:
        kwargs["headers"] = headers

    try:
        r = ident.session.get(url, **kwargs)
    except Exception as e:
        raise RuntimeError(f"Request error: {type(e).__name__}") from e

    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")

    text = r.text.strip()
    if not text.startswith("{"):
        snippet = text[:200].replace("\n", " ")
        raise RuntimeError(f"Blocked / HTML (snippet: {snippet})")
    return r.json()


def fetch_with_retry(venue_code):
    for attempt in range(1, MAX_ATTEMPTS_PER_VENUE + 1):
        try:
            return fetch_api_raw(venue_code)
        except Exception:
            _note_failure()
            if attempt >= MAX_ATTEMPTS_PER_VENUE:
                raise
            delay = min((2 ** attempt) + random.uniform(0, 1), RETRY_BACKOFF_MAX)
            log(f"🔄 Retry {attempt}/{MAX_ATTEMPTS_PER_VENUE} for "
                f"{venue_code} in {delay:.1f}s")
            time.sleep(delay)

# =====================================================
# TIME HELPERS
# =====================================================
def minutes_left(show_time_str):
    try:
        now = datetime.now(IST)
        t = datetime.strptime(show_time_str, "%I:%M %p")
        t = t.replace(
            year=TARGET_DATE.year,
            month=TARGET_DATE.month,
            day=TARGET_DATE.day,
            tzinfo=IST,
        )
        return (t - now).total_seconds() / 60
    except Exception:
        return 9999

# =====================================================
# PARSER
# =====================================================
def parse_payload(data):
    out = []
    sd = data.get("ShowDetails", [])
    if not sd:
        return out

    venue       = sd[0].get("Venues", {})
    venue_name  = venue.get("VenueName", "")
    venue_add   = venue.get("VenueAdd", "")
    chain       = venue.get("VenueCompName", "Unknown")

    for ev in sd[0].get("Event", []):
        title = ev.get("EventTitle", "Unknown")
        for ch in ev.get("ChildEvents", []):
            dim  = ch.get("EventDimension", "").strip()
            lang = ch.get("EventLanguage", "").strip()
            suffix = " | ".join(x for x in (dim, lang) if x)
            movie = f"{title} [{suffix}]" if suffix else title

            for sh in ch.get("ShowTimes", []):
                if sh.get("ShowDateCode") != DATE_CODE:
                    continue
                total = sold = avail = gross = 0
                for cat in sh.get("Categories", []):
                    seats = int(cat.get("MaxSeats", 0))
                    free  = int(cat.get("SeatsAvail", 0))
                    price = float(cat.get("CurPrice", 0))
                    total += seats
                    avail += free
                    sold  += seats - free
                    gross += (seats - free) * price

                out.append({
                    "movie":      movie,
                    "venue":      venue_name,
                    "address":    venue_add,
                    "chain":      chain,
                    "time":       sh.get("ShowTime", ""),
                    "audi":       sh.get("Attributes", "") or "",
                    "session_id": str(sh.get("SessionId", "")),
                    "totalSeats": total,
                    "available":  avail,
                    "sold":       sold,
                    "gross":      round(gross, 2)
                })
    return out

# =====================================================
# STABLE SHOW KEY
# =====================================================
def show_key(r):
    return (r["venue"], r["time"], r["session_id"], r["audi"])

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    log("🚀 BMS DAILY TRACKER STARTED (curl_cffi + Safari impersonation)")
    log(f"   SHARD_ID                = {SHARD_ID}")
    log(f"   MODE                    = {MODE}")
    log(f"   DATE_CODE               = {DATE_CODE}")
    log(f"   BASE_DIR                = {BASE_DIR}")
    log(f"   CUTOFF_MINUTES          = {CUTOFF_MINUTES}")
    log(f"   VENUES_FILE             = {VENUES_FILE}")
    log(f"   USE_XFF                 = {USE_XFF}")
    log(f"   MAX_ATTEMPTS/VENUE      = {MAX_ATTEMPTS_PER_VENUE}")
    log(f"   FAILURE_ROTATE_THRESHOLD= {FAILURE_ROTATE_THRESHOLD}")

    with open(VENUES_FILE, "r", encoding="utf-8") as f:
        venues = json.load(f)

    fetched       = []
    failed        = []
    failure_count = 0

    for i, vcode in enumerate(venues, 1):
        log(f"[{i}/{len(venues)}] {vcode}")

        delay = min(BASE_DELAY + failure_count * 0.2, MAX_DELAY)
        time.sleep(random.uniform(delay, delay + 0.3))

        try:
            raw = fetch_with_retry(vcode)
            for r in parse_payload(raw):
                mins = minutes_left(r["time"])
                if mins <= CUTOFF_MINUTES:
                    r["minsLeft"] = round(mins, 1)
                    r["city"]     = venues[vcode].get("City", "Unknown")
                    r["state"]    = venues[vcode].get("State", "Unknown")
                    r["source"]   = "BMS"
                    r["date"]     = DATE_CODE
                    fetched.append(r)
            failure_count = 0
        except Exception as e:
            failed.append(vcode)
            failure_count += 1
            if failure_count >= FAILURE_THRESHOLD:
                log(f"⛔ {failure_count} consecutive venue failures – "
                    f"cooling down {COOLDOWN_SECONDS}s")
                time.sleep(COOLDOWN_SECONDS)
                rotate_identity("after cooldown")
                failure_count = 0
            log(f"❌ {vcode} failed | {type(e).__name__} | {str(e)[:100]}")

    if failed:
        log(f"⚠️ FINAL FAILED VENUES: {len(failed)}")
        log(f"FAILED LIST: {failed}")

    # =====================================================
    # FINAL DETAILED
    # =====================================================
    if MODE == "advance":
        detailed = fetched
        log(f"🆕 FRESH WRITE (advance) | Shows={len(detailed)}")
    else:
        if os.path.exists(DETAILED_FILE):
            with open(DETAILED_FILE, "r", encoding="utf-8") as f:
                old_rows = json.load(f)
        else:
            old_rows = []

        old_map = {show_key(r): r for r in old_rows}
        new_map = {}

        for r in fetched:
            key = show_key(r)
            if key in old_map:
                old_map[key].update({
                    "totalSeats": r["totalSeats"],
                    "available":  r["available"],
                    "sold":       r["sold"],
                    "gross":      r["gross"],
                    "minsLeft":   r.get("minsLeft")
                })
                new_map[key] = old_map[key]
            else:
                new_map[key] = r

        for key, r in old_map.items():
            if key not in new_map:
                new_map[key] = r

        detailed = list(new_map.values())
        log(f"🔀 MERGED (daily) | old={len(old_rows)} fresh={len(fetched)} "
            f"final={len(detailed)}")

    # =====================================================
    # SUMMARY
    # =====================================================
    summary = {}
    for r in detailed:
        movie = r["movie"]
        city  = r["city"]
        venue = r["venue"]
        total = r["totalSeats"]
        sold  = r["sold"]
        gross = r["gross"]
        occ   = (sold / total * 100) if total else 0

        if movie not in summary:
            summary[movie] = {
                "shows": 0, "gross": 0.0, "sold": 0, "totalSeats": 0,
                "venues": set(), "cities": set(),
                "fastfilling": 0, "housefull": 0
            }
        m = summary[movie]
        m["shows"]      += 1
        m["gross"]      += gross
        m["sold"]       += sold
        m["totalSeats"] += total
        m["venues"].add(venue)
        m["cities"].add(city)
        if occ >= 98:
            m["housefull"]   += 1
        elif occ >= 50:
            m["fastfilling"] += 1

    final_summary = {
        movie: {
            "shows":       m["shows"],
            "gross":       round(m["gross"], 2),
            "sold":        m["sold"],
            "totalSeats":  m["totalSeats"],
            "venues":      len(m["venues"]),
            "cities":      len(m["cities"]),
            "fastfilling": m["fastfilling"],
            "housefull":   m["housefull"],
            "occupancy":   round((m["sold"] / m["totalSeats"]) * 100, 2)
                           if m["totalSeats"] else 0.0
        }
        for movie, m in summary.items()
    }

    # =====================================================
    # SAVE
    # =====================================================
    with open(DETAILED_FILE, "w", encoding="utf-8") as f:
        json.dump(detailed, f, indent=2, ensure_ascii=False)
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        json.dump(final_summary, f, indent=2, ensure_ascii=False)

    log(f"✅ DONE | Shows={len(detailed)} | Movies={len(final_summary)}")
