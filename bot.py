import praw
import requests
import csv
import os
import time
import subprocess
from datetime import datetime

CSV_FILE = "subreddit_refs.csv"

# --------------------------
# File helpers
# --------------------------
def save_rows(rows):
    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["id", "subreddit", "title", "created_utc"])
        writer.writerows(rows)

def load_existing():
    if not os.path.exists(CSV_FILE):
        return set(), None
    ids = set()
    oldest = None
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header
        for row in reader:
            ids.add(row[0])
            ts = int(row[3])
            if oldest is None or ts < oldest:
                oldest = ts
    return ids, oldest

def git_push():
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions[bot]"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions[bot]@users.noreply.github.com"], check=True)
        subprocess.run(["git", "add", CSV_FILE], check=True)
        subprocess.run(["git", "commit", "-m", "Update subreddit_refs.csv [auto]"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✔ CSV pushed to GitHub")
    except subprocess.CalledProcessError:
        print("⚠ Nothing new to commit")

# --------------------------
# API Setup
# --------------------------
reddit = praw.Reddit(
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    username=os.getenv("USERNAME"),
    password=os.getenv("PASSWORD"),
    user_agent=os.getenv("USER_AGENT")
)

subreddit = reddit.subreddit("ofcoursethatsasub")

# --------------------------
# Main logic
# --------------------------
seen_ids, oldest_seen = load_existing()
start_time = time.time()
print(f"📂 Loaded {len(seen_ids)} existing rows. Oldest seen: {oldest_seen}")

cycle = 0
while True:
    cycle += 1
    new_rows = []

    # --- 1. Fetch new posts via Reddit API ---
    print(f"\n🔄 Cycle {cycle}: Checking for new posts...")
    for post in subreddit.new(limit=500):
        if post.id not in seen_ids:
            new_rows.append([
                post.id,
                post.subreddit.display_name,
                post.title.replace("\n", " "),
                int(post.created_utc)
            ])
            seen_ids.add(post.id)

    # --- 2. Backfill older posts via Pushshift ---
    if oldest_seen:
        print("📉 Backfilling older posts...")
        url = f"https://api.pushshift.io/reddit/submission/search/?subreddit=ofcoursethatsasub&before={oldest_seen}&size=100&sort=desc"
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            data = r.json().get("data", [])
            for d in data:
                if d["id"] not in seen_ids:
                    new_rows.append([
                        d["id"],
                        d["subreddit"],
                        d["title"].replace("\n", " "),
                        int(d["created_utc"])
                    ])
                    seen_ids.add(d["id"])
            if data:
                oldest_seen = int(data[-1]["created_utc"])
                print(f"⬅ Oldest now {oldest_seen} ({datetime.utcfromtimestamp(oldest_seen)})")

    # --- 3. Save + commit if new data ---
    if new_rows:
        save_rows(new_rows)
        print(f"💾 Saved {len(new_rows)} new posts. Total now: {len(seen_ids)}")
        git_push()
    else:
        print("✅ No new posts this cycle.")

    # --- 4. Timing ---
    elapsed = time.time() - start_time
    if elapsed > 540:  # after ~9 minutes
        print("⏰ 9 minutes reached → final commit before stopping.")
        git_push()
        break
    if elapsed > 600:  # hard stop at 10 minutes
        print("🛑 10 minute limit reached.")
        break

    time.sleep(1)

