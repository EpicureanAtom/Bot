import requests
import csv
import os
import time
from datetime import datetime
import re

CSV_FILE = "subreddit_refs2.csv"  # <-- ensures CSV is subreddit_refs2.csv
FRESH_START = False      # start fresh or continue from existing CSV
RUN_LIMIT = 1800         # total run time in seconds (30 min)
CYCLE_TIME = 260         # ~4:20 per cycle

SUBREDDIT_NAME = "ofcoursethatsasub"
SUB_PATTERN = re.compile(r"\br/([A-Za-z0-9_]+)\b")  # regex for subreddit mentions

# --------------------------
# File helpers
# --------------------------
def load_existing():
    """Load existing CSV and return rows, seen post IDs, and oldest timestamp."""
    if not os.path.exists(CSV_FILE) or FRESH_START:
        return [], set(), None

    rows, ids = [], set()
    oldest = None
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header
        for row in reader:
            if len(row) < 6:
                continue
            rows.append(row)
            ids.add(row[0])
            try:
                ts = int(row[5])
            except ValueError:
                ts = None
            if ts is not None and (oldest is None or ts < oldest):
                oldest = ts
    return rows, ids, oldest

def save_csv(rows):
    """Save CSV only to subreddit_refs2.csv"""
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["post_id", "type", "context", "subreddit", "author", "timestamp"])
        writer.writerows(rows)

# --------------------------
# Main loop (historical only)
# --------------------------
rows, seen_ids, oldest_seen = load_existing()
start_time = time.time()
cycle = 0

print(f"📂 Loaded {len(seen_ids)} posts. Oldest seen: {oldest_seen}")

while True:
    cycle += 1
    new_rows = []

    print(f"\n🔄 Cycle {cycle}: Backfilling older posts...")
    if oldest_seen is None:
        oldest_seen = int(time.time())

    url = f"https://api.pushshift.io/reddit/submission/search/?subreddit={SUBREDDIT_NAME}&before={oldest_seen}&size=50&sort=desc"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            data = r.json().get("data", [])
            for d in data:
                if d["id"] in seen_ids:
                    continue
                text = f"{d.get('title','')}\n{d.get('selftext','')}"
                matches = SUB_PATTERN.findall(text)
                valid = [m for m in matches if m.lower() != SUBREDDIT_NAME.lower()]
                if valid:
                    context = text[:200].replace("\n", " ")
                    new_rows.append([
                        d["id"],
                        "post",
                        context,
                        f"r/{d['subreddit']}",
                        d.get("author"),
                        int(d["created_utc"])
                    ])
                    seen_ids.add(d["id"])
            if data:
                oldest_seen = int(data[-1]["created_utc"])
                print(f"⬅ Oldest now {oldest_seen} ({datetime.utcfromtimestamp(oldest_seen)})")
    except Exception as e:
        print(f"⚠ Pushshift failed: {e}")

    # Save CSV
    if new_rows:
        rows = new_rows + rows
        save_csv(rows)
        print(f"💾 Saved {len(new_rows)} new rows. Total: {len(rows)}")
    else:
        print("✅ No new historical matches this cycle.")

    # Stop after RUN_LIMIT
    elapsed = time.time() - start_time
    if elapsed >= RUN_LIMIT:
        print("🛑 Time limit reached. Final save + exit.")
        save_csv(rows)
        break

    time.sleep(CYCLE_TIME)
