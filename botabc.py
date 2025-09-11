import requests
import csv
import os
import time
import subprocess
from datetime import datetime
import re

CSV_FILE = "subreddit_refs2.csv"
FRESH_START = False
CHUNK_SIZE = 50
SLEEP_BETWEEN_CHUNKS = 2
MAX_RETRIES = 5

SUBREDDIT_NAME = "ofcoursethatsasub"
SUB_PATTERN = re.compile(r"\br/([A-Za-z0-9_]+)\b")

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
        next(reader, None)
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
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["post_id", "type", "context", "subreddit", "author", "timestamp"])
        writer.writerows(rows)

def commit_push_csv():
    """Commit and push CSV to GitHub if there are changes."""
    try:
        subprocess.run(["git", "add", CSV_FILE], check=True)
        result = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if result.returncode != 0:
            subprocess.run(["git", "commit", "-m", "Update subreddit_refs2.csv [auto]"], check=True)
            subprocess.run(["git", "pull", "--rebase", "--autostash"], check=True)
            subprocess.run(["git", "push"], check=True)
            print("💾 CSV committed and pushed")
        else:
            print("✅ No new changes to commit")
    except subprocess.CalledProcessError as e:
        print(f"⚠ Git operation failed: {e}")

# --------------------------
# Pushshift fetch
# --------------------------
def fetch_posts(before_timestamp):
    url = f"https://api.pushshift.io/reddit/submission/search/?subreddit={SUBREDDIT_NAME}&before={before_timestamp}&size={CHUNK_SIZE}&sort=desc"
    retries = 0
    while retries < MAX_RETRIES:
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                return r.json().get("data", [])
            else:
                print(f"⚠ Bad response {r.status_code}, retrying...")
        except Exception as e:
            print(f"⚠ Pushshift request failed: {e}")
        retries += 1
        time.sleep(2 ** retries)
    return []

# --------------------------
# Main loop
# --------------------------
rows, seen_ids, oldest_seen = load_existing()
if oldest_seen is None:
    oldest_seen = int(time.time())

print(f"📂 Loaded {len(seen_ids)} posts. Starting from timestamp: {oldest_seen}")

while True:
    print(f"\n🔄 Fetching posts before {oldest_seen} ({datetime.utcfromtimestamp(oldest_seen)})")
    data = fetch_posts(oldest_seen)

    if not data:
        print("✅ No more posts returned. Backfill complete.")
        save_csv(rows)
        commit_push_csv()
        break

    new_rows = []
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

    if new_rows:
        rows = new_rows + rows
        save_csv(rows)
        print(f"💾 Saved {len(new_rows)} new rows. Total rows: {len(rows)}")
        commit_push_csv()  # <-- commit after every chunk
    else:
        print("⚠ No new valid matches this chunk.")

    oldest_seen = int(data[-1]["created_utc"])
    time.sleep(SLEEP_BETWEEN_CHUNKS)
