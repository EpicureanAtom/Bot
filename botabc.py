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
    """Load CSV if exists. Returns: rows, seen_ids, newest_timestamp"""
    if not os.path.exists(CSV_FILE) or FRESH_START:
        return [], set(), None
    rows, ids = [], set()
    newest = None
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
            if ts is not None and (newest is None or ts > newest):
                newest = ts
    return rows, ids, newest

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
# Pushshift fetch (forward crawl)
# --------------------------
def fetch_posts(after_timestamp, until_timestamp):
    url = (
        f"https://api.pushshift.io/reddit/submission/search/"
        f"?subreddit={SUBREDDIT_NAME}&after={after_timestamp}"
        f"&before={until_timestamp}&size={CHUNK_SIZE}&sort=asc"
    )
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
# Main loop (resume forward crawl)
# --------------------------
rows, seen_ids, newest_seen = load_existing()
if newest_seen is None:
    newest_seen = 0  # start from the beginning

end_time = int(time.time())  # cap crawl at "now"

print(f"📂 Loaded {len(seen_ids)} posts. Resuming from {newest_seen} ({datetime.utcfromtimestamp(newest_seen)}), until {end_time}")

while True:
    print(f"\n🔄 Fetching posts after {newest_seen} ({datetime.utcfromtimestamp(newest_seen)})")
    data = fetch_posts(newest_seen, end_time)

    if not data:
        print("✅ No more posts returned. Forward fill complete.")
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
        rows.extend(new_rows)  # append chronologically
        save_csv(rows)
        print(f"💾 Saved {len(new_rows)} new rows. Total rows: {len(rows)}")
        commit_push_csv()
    else:
        print("⚠ No new valid matches this chunk.")

    # Stop condition: if Pushshift repeats the same timestamp
    latest_in_chunk = int(data[-1]["created_utc"])
    if latest_in_chunk <= newest_seen:
        print("🛑 Stopping: timestamp did not advance (likely end of results).")
        break

    newest_seen = latest_in_chunk
    time.sleep(SLEEP_BETWEEN_CHUNKS)
