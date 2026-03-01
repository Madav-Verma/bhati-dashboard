import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
from datetime import datetime, timezone, timedelta
import json
import os

# ══════════════════════════════════════════════════════════════════════════════
#   CONFIG
# ══════════════════════════════════════════════════════════════════════════════
SCAN_COLLECTION  = "Bhati-March-2026"
USER_COLLECTION  = "User"
LAST_FETCH_FILE  = "last_fetch.json"
USER_CACHE_FILE  = "user_cache.json"
SEWADAR_EXCEL    = "Sewadar_Details.xlsx"
OUTPUT_JSON      = "data.json"
TIMESTAMP_FIELD  = "timestamp"
USER_CACHE_HOURS = 24   # Refresh user cache once every 24 hours

# ══════════════════════════════════════════════════════════════════════════════
#   FIREBASE INIT
# ══════════════════════════════════════════════════════════════════════════════
if not firebase_admin._apps:
    secret = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
    if secret:
        import tempfile
        sa = json.loads(secret)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(sa, f)
            tmp_path = f.name
        cred = credentials.Certificate(tmp_path)
    else:
        cred = credentials.Certificate(
            r"C:\Users\verma\Desktop\Dashboards\USER - Gameplay\service_account.json"
        )
    firebase_admin.initialize_app(cred)

db = firestore.client()

# ══════════════════════════════════════════════════════════════════════════════
#   LOAD SEWADAR EXCEL LOOKUP (badge → name, centre, department)
# ══════════════════════════════════════════════════════════════════════════════
badge_lookup = {}
if os.path.exists(SEWADAR_EXCEL):
    df_sew = pd.read_excel(SEWADAR_EXCEL)
    df_sew["Badge Number"] = df_sew["Badge Number"].astype(str).str.strip()
    for _, row in df_sew.iterrows():
        badge_lookup[row["Badge Number"]] = {
            "name":          str(row.get("Name of Sewadar", "")).strip(),
            "gender":        str(row.get("Gender", "")).strip(),
            "satsang_point": str(row.get("Satsang Point", "")).strip(),
            "centre":        str(row.get("Centre", "")).strip(),
            "department":    str(row.get("Deployed Department", "")).strip(),
        }
    print(f"Badge lookup loaded: {len(badge_lookup)} sewadars from Excel.")
else:
    print("WARNING: Sewadar_Details.xlsx not found — names will be unknown.")

def lookup_badge(badge_no):
    return badge_lookup.get(str(badge_no).strip(), {
        "name": "Unknown", "gender": "", "satsang_point": "",
        "centre": "", "department": ""
    })

# ══════════════════════════════════════════════════════════════════════════════
#   USER CACHE (only refresh every 24 hours to save reads)
# ══════════════════════════════════════════════════════════════════════════════
def load_user_cache():
    if os.path.exists(USER_CACHE_FILE):
        with open(USER_CACHE_FILE, "r") as f:
            cache = json.load(f)
        cached_at = datetime.fromisoformat(cache.get("cached_at", "2000-01-01"))
        if cached_at.tzinfo is None:
            cached_at = cached_at.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - cached_at).total_seconds() / 3600
        if age_hours < USER_CACHE_HOURS:
            print(f"User cache valid (age: {age_hours:.1f}h) — skipping Firestore user reads.")
            return cache.get("users", {})
    return None

def fetch_and_cache_users():
    print("Fetching User collection from Firestore (cache expired)...")
    user_map = {}
    for doc in db.collection(USER_COLLECTION).stream():
        d = doc.to_dict()
        user_map[doc.id] = {
            "displayName": d.get("displayName", ""),
            "badgeNumber": d.get("badgeNumber", ""),
            "email":       d.get("email", ""),
            "role":        d.get("role", ""),
            "center":      d.get("center", ""),
        }
    with open(USER_CACHE_FILE, "w") as f:
        json.dump({"cached_at": datetime.now(timezone.utc).isoformat(), "users": user_map}, f, indent=2)
    print(f"  Cached {len(user_map)} users.")
    return user_map

user_cache = load_user_cache()
if user_cache is None:
    user_cache = fetch_and_cache_users()

# userId → badgeNumber reverse lookup
userid_to_badge = {uid: info.get("badgeNumber", "") for uid, info in user_cache.items()}

# ══════════════════════════════════════════════════════════════════════════════
#   TIMESTAMP HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def load_last_fetch():
    if os.path.exists(LAST_FETCH_FILE):
        with open(LAST_FETCH_FILE, "r") as f:
            data = json.load(f)
            dt = datetime.fromisoformat(data["last_fetch"])
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    return None

def save_last_fetch(dt):
    with open(LAST_FETCH_FILE, "w") as f:
        json.dump({"last_fetch": dt.isoformat()}, f, indent=2)

# ══════════════════════════════════════════════════════════════════════════════
#   FETCH SCANS (incremental)
# ══════════════════════════════════════════════════════════════════════════════
last_fetch = load_last_fetch()
fetch_time = datetime.now(timezone.utc)

if last_fetch:
    print(f"Incremental fetch — after: {last_fetch.strftime('%d-%m-%Y %I:%M %p UTC')}")
    query = db.collection(SCAN_COLLECTION).where(filter=firestore.FieldFilter(TIMESTAMP_FIELD, ">", last_fetch))
else:
    print("First run — fetching ALL scans...")
    query = db.collection(SCAN_COLLECTION)

new_scans = []
skipped   = 0

for doc in query.stream():
    d = doc.to_dict()

    if TIMESTAMP_FIELD not in d:
        print(f"  Skipped {doc.id} — missing timestamp")
        skipped += 1
        continue

    ts = d[TIMESTAMP_FIELD]
    if hasattr(ts, "tzinfo") and ts.tzinfo is not None:
        ts_naive = ts.replace(tzinfo=None)
    else:
        ts_naive = ts

    # Barcode from scan doc
    barcode    = str(d.get("barcode", "")).strip()
    scanned_by = str(d.get("scannedBy", "")).strip()
    user_id    = d.get("userId", "")

    # If barcode missing, fall back to userId → badge lookup
    if not barcode:
        barcode = userid_to_badge.get(user_id, "")

    # Resolve names/centres from Excel
    sewadar_info   = lookup_badge(barcode)
    scanner_info   = lookup_badge(scanned_by)

    new_scans.append({
        "scan_id":          doc.id,
        "userId":           user_id,
        # Scanned sewadar
        "badge_no":         barcode,
        "sewadar_name":     sewadar_info["name"],
        "sewadar_centre":   sewadar_info["centre"],
        "satsang_point":    sewadar_info["satsang_point"],
        "department":       sewadar_info["department"],
        "gender":           sewadar_info["gender"],
        # Scan details
        "type":             d.get("type", ""),
        "scanned_at_loc":   d.get("scannedAt", ""),
        "timestamp":        ts_naive.isoformat(),
        "date":             ts_naive.strftime("%Y-%m-%d"),
        "time":             ts_naive.strftime("%H:%M:%S"),
        # Scanner info
        "scanned_by_badge": scanned_by,
        "scanned_by_name":  scanner_info["name"],
        "scanned_by_centre":scanner_info["centre"],
    })

print(f"New scans fetched: {len(new_scans)}")

# ══════════════════════════════════════════════════════════════════════════════
#   MERGE WITH EXISTING
# ══════════════════════════════════════════════════════════════════════════════
existing_scans = []
if os.path.exists(OUTPUT_JSON):
    with open(OUTPUT_JSON, "r") as f:
        existing_data = json.load(f)
        existing_scans = existing_data.get("scans", [])

def normalize_scan(s):
    """Normalize old scan records that used different field names."""
    # Old script used 'badgeNumber', new uses 'badge_no'
    if "badge_no" not in s:
        s["badge_no"] = s.get("badgeNumber", s.get("barcode", ""))
    if "sewadar_name" not in s:
        s["sewadar_name"] = s.get("displayName", "Unknown")
    if "sewadar_centre" not in s:
        s["sewadar_centre"] = s.get("center", s.get("centre", ""))
    if "department" not in s:
        # Try to look up from Excel if we have the badge
        info = lookup_badge(s.get("badge_no", ""))
        s["department"]    = info.get("department", "")
        s["satsang_point"] = info.get("satsang_point", "")
        s["gender"]        = info.get("gender", "")
    if "scanned_by_badge" not in s:
        s["scanned_by_badge"]  = s.get("scannedBy", "")
        scanner = lookup_badge(s["scanned_by_badge"])
        s["scanned_by_name"]   = scanner.get("name", "—")
        s["scanned_by_centre"] = scanner.get("centre", "")
    return s

# Normalize existing records and merge new ones
existing_scans = [normalize_scan(s) for s in existing_scans]
existing_ids   = {s["scan_id"] for s in existing_scans}
for s in new_scans:
    if s["scan_id"] not in existing_ids:
        existing_scans.append(s)

all_scans = existing_scans
print(f"Total scans in dataset: {len(all_scans)}")

# ══════════════════════════════════════════════════════════════════════════════
#   ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
all_dates          = []
sewadar_attendance = []
headcount_per_day  = []
centre_summary     = []

if all_scans:
    df = pd.DataFrame(all_scans)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["date"]      = df["timestamp"].dt.strftime("%Y-%m-%d")
    df["time"]      = df["timestamp"].dt.strftime("%H:%M:%S")
    df              = df.sort_values("timestamp")

    all_dates = sorted(df["date"].unique().tolist())

    # ── Per Sewadar per Date ──────────────────────────────────────────────────
    for badge, grp in df.groupby("badge_no"):
        row = {
            "badge_no":       badge,
            "sewadar_name":   grp["sewadar_name"].iloc[0],
            "sewadar_centre": grp["sewadar_centre"].iloc[0],
            "satsang_point":  grp["satsang_point"].iloc[0],
            "department":     grp["department"].iloc[0],
        }
        for date in all_dates:
            day = grp[grp["date"] == date]
            if day.empty:
                row[date] = {"status": "ABSENT", "first_in": "-", "last_out": "-"}
            else:
                ins  = day[day["type"] == "IN"]["time"].tolist()
                outs = day[day["type"] == "OUT"]["time"].tolist()
                row[date] = {
                    "status":   "PRESENT",
                    "first_in":  min(ins)  if ins  else "-",
                    "last_out":  max(outs) if outs else "-",
                }
        sewadar_attendance.append(row)

    # ── Headcount per Day ─────────────────────────────────────────────────────
    for date in all_dates:
        day_df = df[df["date"] == date]
        headcount_per_day.append({
            "date":          date,
            "total_present": int(day_df["badge_no"].nunique()),
            "total_in":      int(day_df[day_df["type"] == "IN"]["badge_no"].nunique()),
            "total_out":     int(day_df[day_df["type"] == "OUT"]["badge_no"].nunique()),
        })

    # ── Centre-wise Summary ───────────────────────────────────────────────────
    all_centres = sorted(df["sewadar_centre"].dropna().unique().tolist())
    for centre in all_centres:
        if not centre:
            continue
        c_df = df[df["sewadar_centre"] == centre]
        c_row = {
            "centre":        centre,
            "total_sewadars": int(c_df["badge_no"].nunique()),
            "dates": {}
        }
        for date in all_dates:
            day = c_df[c_df["date"] == date]
            c_row["dates"][date] = {
                "present": int(day["badge_no"].nunique()),
                "in":      int(day[day["type"] == "IN"]["badge_no"].nunique()),
                "out":     int(day[day["type"] == "OUT"]["badge_no"].nunique()),
            }
        centre_summary.append(c_row)

    # ── Raw log ───────────────────────────────────────────────────────────────
    raw_log = df.sort_values("timestamp", ascending=False).to_dict(orient="records")

else:
    raw_log = []

# ══════════════════════════════════════════════════════════════════════════════
#   EXPORT JSON
# ══════════════════════════════════════════════════════════════════════════════
output = {
    "last_updated":       fetch_time.strftime("%d-%m-%Y %I:%M %p UTC"),
    "total_scans":        len(all_scans),
    "total_sewadars":     len(sewadar_attendance),
    "dates":              all_dates,
    "scans":              all_scans,
    "sewadar_attendance": sewadar_attendance,
    "headcount_per_day":  headcount_per_day,
    "centre_summary":     centre_summary,
    "raw_log":            raw_log,
}

with open(OUTPUT_JSON, "w") as f:
    json.dump(output, f, indent=2, default=str)

save_last_fetch(fetch_time)
print(f"\nDone. {len(all_scans)} scans | {len(sewadar_attendance)} sewadars | {len(centre_summary)} centres.")
print(f"Next run fetches after: {fetch_time.strftime('%d-%m-%Y %I:%M %p UTC')}")
