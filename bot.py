import praw
import requests
import csv
import os
import time
import subprocess
from datetime import datetime

CSV_FILE = "subreddit_reffss.csv"
RUN_LIMIT = 600   # 10 minutes
COMMIT_TIME = 540 # commit around 9 minutes

# --------------------------
# File helpers
# --------------------------
def save_rows(rows):
    """Insert new rows at the top of the CSV so newest posts appear first."""
    if not rows:
        return
    file_exists = os.path.isfile(CSV_FILE)
    
    # Load existing CSV
    existing_lines = []
    if file_exists:
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            existing_lines = f.readlines()
    
    # Prepare new CSV content
    header = existing_lines[0] if existing_lines else "id,subreddit,title,created_utc\n"
    new_lines = [",".join([row[0], row[1], row[2].replace(",", " "), str(row[3])]) + "\n" for row in rows]
    
    # Write new content at the top
    with open(CSV_FILE, "w", encoding="utf-8") as f:
        f.write(header)
        f.writelines(new_lines)
        f.writelines(existing_lines[1:] if existing_lines else [])

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
# Reddit setup
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
# Main loop
# --------------------------
seen_ids, oldest_seen = load_existing()
start_time = time.time()
committed = False
cycle = 0

print(f"📂 Loaded {len(seen_ids)} existing rows. Oldest seen: {oldest_seen}")

while True:
    cycle += 1
    new_rows = []

    # --- 1. Fetch new posts via Reddit API ---
    print(f"\n🔄 Cycle {cycle}: Checking for new posts...")
    for post in subreddit.new(limit=500):
        if post.id not in seen_ids:
            new_rows.append([
                post.id,
                f"r/{post.subreddit.display_name}",
                post.title.replace("\n", " "),
                int(post.created_utc)
            ])
            seen_ids.add(post.id)

    # --- 2. Backfill older posts via Pushshift ---
    if oldest_seen:
        print("📉 Backfilling older posts...")
        url = f"https://api.pushshift.io/reddit/submission/search/?subreddit=ofcoursethatsasub&before={oldest_seen}&size=100&sort=desc"
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                data = r.json().get("data", [])
                for d in data:
                    if d["id"] not in seen_ids:
                        new_rows.append([
                            d["id"],
                            f"r/{d['subreddit']}",
                            d["title"].replace("\n", " "),
                            int(d["created_utc"])
                        ])
                        seen_ids.add(d["id"])
                if data:
                    oldest_seen = int(data[-1]["created_utc"])
                    print(f"⬅ Oldest now {oldest_seen} ({datetime.utcfromtimestamp(oldest_seen)})")
        except Exception as e:
            print(f"⚠ Pushshift request failed: {e}")

    # --- 3. Save + commit if new data ---
    if new_rows:
        save_rows(new_rows)
        print(f"💾 Saved {len(new_rows)} new posts. Total now: {len(seen_ids)}")
        git_push()
    else:
        print("✅ No new posts this cycle.")

    # --- 4. Timing & limits ---
    elapsed = time.time() - start_time
    if not committed and elapsed >= 540:  # ~9 minutes
        print("⏰ 9 minutes reached → final commit before stopping.")
        git_push()
        committed = True
    if elapsed >= RUN_LIMIT:
        if not committed:
            git_push()
        print("🛑 10 minute limit reached. Exiting.")
        break

    time.sleep(1)
