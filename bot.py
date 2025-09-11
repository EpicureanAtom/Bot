import praw
import requests
import csv
import os
import time
import subprocess
import re
from datetime import datetime

CSV_FILE = "subreddit_reffs.csv"
COMMIT_TIME = 540  # commit at ~9 minutes
SLEEP_INTERVAL = 1  # 1 second between cycles

# --------------------------
# File helpers
# --------------------------
def save_rows(rows):
    if not rows:
        return
    file_exists = os.path.isfile(CSV_FILE)
    existing_lines = []
    if file_exists:
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            existing_lines = f.readlines()
    header = existing_lines[0] if existing_lines else "id,type,subreddit,title_or_body,created_utc,mentioned_subs\n"
    new_lines = [
        ",".join([
            row[0],
            row[1],  # type: submission/comment
            row[2].replace(",", " "),
            row[3].replace("\n", " "),
            str(row[4]),
            "|".join(row[5])
        ]) + "\n"
        for row in rows
    ]
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
            ts = int(row[4])
            if oldest is None or ts < oldest:
                oldest = ts
    return ids, oldest

def git_push(force=False):
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions[bot]"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions[bot]@users.noreply.github.com"], check=True)
        subprocess.run(["git", "add", CSV_FILE], check=True)
        subprocess.run(["git", "commit", "-m", "Update subreddit_reffs.csv [auto]"], check=False)
        subprocess.run(["git", "push"], check=True)
        print("✔ CSV pushed to GitHub")
    except subprocess.CalledProcessError:
        print("⚠ Nothing new to commit, continuing...")

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

try:
    while True:
        cycle += 1
        new_rows = []

        # --- 1. Fetch new submissions ---
        print(f"\n🔄 Cycle {cycle}: Checking for new submissions...")
        for post in subreddit.new(limit=500):
            if post.id in seen_ids:
                continue

            mentions = re.findall(r"r/\w+", post.title)
            mentions = [m for m in mentions if m.lower() != "r/ofcoursethatsasub"]
            if not mentions:
                continue

            new_rows.append([
                post.id,
                "submission",
                f"r/{post.subreddit.display_name}",
                post.title,
                int(post.created_utc),
                mentions
            ])
            seen_ids.add(post.id)

        # --- 2. Fetch new comments ---
        print("💬 Checking comments...")
        for comment in subreddit.comments(limit=500):
            if comment.id in seen_ids:
                continue
            mentions = re.findall(r"r/\w+", comment.body)
            mentions = [m for m in mentions if m.lower() != "r/ofcoursethatsasub"]
            if not mentions:
                continue
            new_rows.append([
                comment.id,
                "comment",
                f"r/{comment.subreddit.display_name}",
                comment.body,
                int(comment.created_utc),
                mentions
            ])
            seen_ids.add(comment.id)

        # --- 3. Backfill older submissions via Pushshift ---
        if oldest_seen:
            print("📉 Backfilling older submissions...")
            url = f"https://api.pushshift.io/reddit/submission/search/?subreddit=ofcoursethatsasub&before={oldest_seen}&size=100&sort=desc"
            try:
                r = requests.get(url, timeout=30)
                if r.status_code == 200:
                    data = r.json().get("data", [])
                    for d in data:
                        if d["id"] in seen_ids:
                            continue
                        mentions = re.findall(r"r/\w+", d["title"])
                        mentions = [m for m in mentions if m.lower() != "r/ofcoursethatsasub"]
                        if not mentions:
                            continue
                        new_rows.append([
                            d["id"],
                            "submission",
                            f"r/{d['subreddit']}",
                            d["title"],
                            int(d["created_utc"]),
                            mentions
                        ])
                        seen_ids.add(d["id"])
                    if data:
                        oldest_seen = int(data[-1]["created_utc"])
                        print(f"⬅ Oldest now {oldest_seen} ({datetime.utcfromtimestamp(oldest_seen)})")
            except Exception as e:
                print(f"⚠ Pushshift request failed: {e}")

        # --- 4. Save + commit ---
        if new_rows:
            save_rows(new_rows)
            print(f"💾 Saved {len(new_rows)} new posts/comments. Total now: {len(seen_ids)}")
            git_push()
        else:
            print("✅ No new posts/comments this cycle.")

        # --- 5. Commit at 9 minutes even if nothing new ---
        elapsed = time.time() - start_time
        if not committed and elapsed >= COMMIT_TIME:
            print("⏰ 9 minutes reached → committing CSV even if nothing new")
            git_push(force=True)
            committed = True

        time.sleep(SLEEP_INTERVAL)

except Exception as e:
    print(f"❌ Script error: {e}")
