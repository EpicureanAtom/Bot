import requests
import csv
import os
import time
import subprocess
from datetime import datetime
import re

CSV_FILE = "subreddit_refs2.csv"
FRESH_START = True      # start fresh or continue
RUN_LIMIT = 1800        # total run time in seconds (30 min)
CYCLE_TIME = 260        # ~4:20 per cycle
COMMIT_TIME = 540       # force commit around 9 min

SUBREDDIT_NAME = "ofcoursethatsasub"
SUB_PATTERN = re.compile(r"\br/([A-Za-z0-9_]+)\b")  # regex for subreddit mentions

# --------------------------
# File helpers
# --------------------------
def load_existing():
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
    """Save CSV and commit/push changes."""
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["post_id", "type", "context", "subreddit", "author", "timestamp"])
        writer.writerows(rows)

    try:
        subprocess.run(["git", "add", CSV_FILE], check=True)
        subprocess.run(["git", "commit", "-m", "Cycle update [auto]"], check=True)
    except subprocess.CalledProcessError:
        print("⚠ Nothing new to commit, skipping commit.")

    try:
        subprocess.run(["git", "pull", "--rebase", "--autostash"], check=True)
    except subprocess.CalledProcessError:
        print("⚠ Git pull failed, continuing anyway.")

    try:
        subprocess.run(["git", "push", "origin", "main"], check=True)
    except subprocess.CalledProcessError:
        print("⚠ Git push failed, continuing anyway.")

# --------------------------
# Main loop (historical only)
# --------------------------
rows, seen_ids, oldest_seen = load_existing()
start_time = time.time()
committed = False
cycle = 0

print(f"📂 Loaded {len(seen_ids)} posts. Oldest seen: {oldest_seen}")

while True:
    cycle += 1
    new_rows = []

    print(f"\n🔄 Cycle {cycle}: Backfilling older posts...")
    if oldest_seen is None:
        # If no previous data, start from current time
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
                    new_rows.append([d["id"], "post", context, f"r/{d['subreddit']}", d.get("author"), int(d["created_utc"])])
                    seen_ids.add(d["id"])
            if data:
                oldest_seen = int(data[-1]["created_utc"])
                print(f"⬅ Oldest now {oldest_seen} ({datetime.utcfromtimestamp(oldest_seen)})")
    except Exception as e:
        print(f"⚠ Pushshift failed: {e}")

    # Save + commit
    if new_rows:
        rows = new_rows + rows
        save_csv(rows)
        print(f"💾 Saved {len(new_rows)} new rows. Total: {len(rows)}")
    else:
        print("✅ No new historical matches this cycle.")

    # Force commit at 9 minutes even if no new rows
    elapsed = time.time() - start_time
    if not committed and elapsed >= COMMIT_TIME:
        print("⏰ 9 min reached → force commit")
        save_csv(rows)
        committed = True

    if elapsed >= RUN_LIMIT:
        print("🛑 Time limit reached. Final save + exit.")
        save_csv(rows)
        break

    time.sleep(CYCLE_TIME)
