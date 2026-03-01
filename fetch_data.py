import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
from datetime import datetime, timezone
import json
import os

# ══════════════════════════════════════════════════════════════════════════════
#   CONFIG
# ══════════════════════════════════════════════════════════════════════════════
SCAN_COLLECTION   = "Bhati-March-2026"
USER_COLLECTION   = "User"
LAST_FETCH_FILE   = "last_fetch.json"
OUTPUT_JSON       = "data.json"
TIMESTAMP_FIELD   = "timestamp"

# ══════════════════════════════════════════════════════════════════════════════
#   FIREBASE INIT — reads from GitHub Secret (env var) or local file
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
#   TIMESTAMP HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def load_last_fetch():
    if os.path.exists(LAST_FETCH_FILE):
        with open(LAST_FETCH_FILE, "r") as f:
            data = json.load(f)
            dt = datetime.fromisoformat(data["last_fetch"])
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    return None

def save_last_fetch(dt: datetime):
    with open(LAST_FETCH_FILE, "w") as f:
        json.dump({"last_fetch": dt.isoformat()}, f, indent=2)

# ══════════════════════════════════════════════════════════════════════════════
#   FETCH USERS (for name lookup)
# ══════════════════════════════════════════════════════════════════════════════
print("Fetching user registry...")
user_map = {}
for doc in db.collection(USER_COLLECTION).stream():
    d = doc.to_dict()
    user_map[doc.id] = {
        "displayName": d.get("displayName", "Unknown"),
        "badgeNumber": d.get("badgeNumber", ""),
        "center":      d.get("center", ""),
        "role":        d.get("role", ""),
        "email":       d.get("email", ""),
    }
print(f"  Loaded {len(user_map)} users.")

# ══════════════════════════════════════════════════════════════════════════════
#   FETCH SCANS (incremental)
# ══════════════════════════════════════════════════════════════════════════════
last_fetch = load_last_fetch()
fetch_time = datetime.now(timezone.utc)

if last_fetch:
    print(f"Incremental fetch — after: {last_fetch.strftime('%d-%m-%Y %I:%M %p')}")
    query = db.collection(SCAN_COLLECTION).where(TIMESTAMP_FIELD, ">", last_fetch)
else:
    print("First run — fetching ALL scans...")
    query = db.collection(SCAN_COLLECTION)

new_scans = []
skipped   = 0

for doc in query.stream():
    d = doc.to_dict()

    if TIMESTAMP_FIELD not in d:
        print(f"  Skipped scan {doc.id} (missing timestamp)")
        skipped += 1
        continue

    ts = d[TIMESTAMP_FIELD]
    if hasattr(ts, "tzinfo") and ts.tzinfo is not None:
        ts_naive = ts.replace(tzinfo=None)
    else:
        ts_naive = ts

    user_id   = d.get("userId", "")
    user_info = user_map.get(user_id, {})

    new_scans.append({
        "scan_id":     doc.id,
        "userId":      user_id,
        "displayName": user_info.get("displayName", "Unknown"),
        "badgeNumber": d.get("barcode", user_info.get("badgeNumber", "")),
        "center":      user_info.get("center", ""),
        "role":        user_info.get("role", ""),
        "scannedAt":   d.get("scannedAt", ""),
        "scannedBy":   d.get("scannedBy", ""),
        "type":        d.get("type", ""),
        "timestamp":   ts_naive.isoformat() if isinstance(ts_naive, datetime) else str(ts_naive),
        "date":        ts_naive.strftime("%Y-%m-%d") if isinstance(ts_naive, datetime) else "",
        "time":        ts_naive.strftime("%H:%M:%S") if isinstance(ts_naive, datetime) else "",
    })

print(f"  New scans: {len(new_scans)}")

# ══════════════════════════════════════════════════════════════════════════════
#   MERGE WITH EXISTING DATA
# ══════════════════════════════════════════════════════════════════════════════
existing_scans = []
if os.path.exists(OUTPUT_JSON):
    with open(OUTPUT_JSON, "r") as f:
        existing_data = json.load(f)
        existing_scans = existing_data.get("scans", [])

all_scan_ids = {s["scan_id"] for s in existing_scans}
for s in new_scans:
    if s["scan_id"] not in all_scan_ids:
        existing_scans.append(s)

all_scans = existing_scans
print(f"  Total scans in dataset: {len(all_scans)}")

# ══════════════════════════════════════════════════════════════════════════════
#   ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
if all_scans:
    df = pd.DataFrame(all_scans)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["date"]      = df["timestamp"].dt.strftime("%Y-%m-%d")
    df["time"]      = df["timestamp"].dt.strftime("%H:%M:%S")
    df              = df.sort_values("timestamp")

    all_dates = sorted(df["date"].unique().tolist())

    # Per Sewadar per Date
    sewadar_attendance = []
    for user_id, group in df.groupby("userId"):
        row = {
            "userId":      user_id,
            "displayName": group["displayName"].iloc[0],
            "badgeNumber": group["badgeNumber"].iloc[0],
            "center":      group["center"].iloc[0],
        }
        for date in all_dates:
            day_scans = group[group["date"] == date]
            if day_scans.empty:
                row[date] = {"status": "ABSENT", "first_in": "-", "last_out": "-"}
            else:
                ins  = day_scans[day_scans["type"] == "IN"]["time"].tolist()
                outs = day_scans[day_scans["type"] == "OUT"]["time"].tolist()
                row[date] = {
                    "status":    "PRESENT",
                    "first_in":  min(ins)  if ins  else "-",
                    "last_out":  max(outs) if outs else "-",
                }
        sewadar_attendance.append(row)

    # Headcount per Day
    headcount = []
    for date in all_dates:
        day_df = df[df["date"] == date]
        headcount.append({
            "date":          date,
            "total_present": int(day_df["userId"].nunique()),
            "total_in":      int(day_df[day_df["type"] == "IN"]["userId"].nunique()),
            "total_out":     int(day_df[day_df["type"] == "OUT"]["userId"].nunique()),
        })

    raw_log = df.sort_values("timestamp", ascending=False).to_dict(orient="records")

else:
    all_dates = []
    sewadar_attendance = []
    headcount = []
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
    "headcount_per_day":  headcount,
    "raw_log":            raw_log,
}

with open(OUTPUT_JSON, "w") as f:
    json.dump(output, f, indent=2, default=str)

save_last_fetch(fetch_time)
print(f"\nDone — {len(all_scans)} total scans, {len(sewadar_attendance)} sewadars.")
print(f"Next run fetches after: {fetch_time.strftime('%d-%m-%Y %I:%M %p')}")
