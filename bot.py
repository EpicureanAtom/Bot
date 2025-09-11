import praw
import requests
import csv
import os
import time
import subprocess
from datetime import datetime

CSV_FILE = "subreddit_refs.csv"
RUN_LIMIT = 600    # 10 minutes
COMMIT_TIME = 540  # commit after ~9 minutes
NEW_POST_LIMIT = 50
BACKFILL_BATCH = 100
SUBREDDIT_NAME = "ofcoursethatsasub"
IGNORE_SUB = "r/ofcoursethatsasub"

# --------------------------
# File helpers
# --------------------------
def init_csv():
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", encoding="utf-8") as f:
            f.write("id,subreddit,title,body,created_utc,context\n")

def save_rows(rows):
    if not rows:
        return
    file_exists = os.path.isfile(CSV_FILE)
    existing_lines = []
    if file_exists:
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            existing_lines = f.readlines()
    header = existing_lines[0] if existing_lines else "id,subreddit,title,body,created_utc,context\n"
    new_lines = [",".join([row[0], row[1], row[2].replace(",", " "), 
                           row[3].replace(",", " "), str(row[4]), row[5].replace(",", " ")]) + "\n" 
                 for row in rows]
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
        next(reader, None)
        for row in reader:
            ids.add(row[0])
            ts = int(row[4])
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

subreddit = reddit.subreddit(SUBREDDIT_NAME)

# --------------------------
# Main loop
# --------------------------
init_csv()
seen_ids, oldest_seen = load_existing()
start_time = time.time()
committed = False
cycle = 0

print(f"📂 Loaded {len(seen_ids)} existing rows. Oldest seen: {oldest_seen}")

while True:
    cycle += 1
    new_rows = []

    print(f"\n🔄 Cycle {cycle}: Checking for new posts...")

    # --- 1. Fetch newest posts ---
    for post in subreddit.new(limit=NEW_POST_LIMIT):
        if post.id not in seen_ids:
            content_to_scan = post.title + " " + (post.selftext or "")
            if IGNORE_SUB not in content_to_scan and "r/" in content_to_scan:
                context_snippet = content_to_scan[:100]
                new_rows.append([
                    post.id,
                    f"r/{post.subreddit.display_name}",
                    post.title.replace("\n", " "),
                    post.selftext.replace("\n", " ") if post.selftext else "",
                    int(post.created_utc),
                    context_snippet
                ])
                seen_ids.add(post.id)

            # --- check comments ---
            post.comments.replace_more(limit=0)
            for comment in post.comments.list():
                if comment.id not in seen_ids:
                    if IGNORE_SUB not in comment.body and "r/" in comment.body:
                        snippet = comment.body[:100]
                        new_rows.append([
                            comment.id,
                            f"r/{post.subreddit.display_name}",
                            post.title.replace("\n", " "),
                            comment.body.replace("\n", " "),
                            int(comment.created_utc),
                            snippet
                        ])
                        seen_ids.add(comment.id)

    # --- 2. Backfill older posts via Pushshift ---
    if oldest_seen:
        print("📉 Backfilling older posts...")
        url = f"https://api.pushshift.io/reddit/submission/search/?subreddit={SUBREDDIT_NAME}&before={oldest_seen}&size={BACKFILL_BATCH}&sort=desc"
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                data = r.json().get("data", [])
                for d in data:
                    if d["id"] not in seen_ids:
                        content_to_scan = d["title"] + " " + (d.get("selftext") or "")
                        if IGNORE_SUB not in content_to_scan and "r/" in content_to_scan:
                            snippet = content_to_scan[:100]
                            new_rows.append([
                                d["id"],
                                f"r/{d['subreddit']}",
                                d["title"].replace("\n", " "),
                                d.get("selftext", "").replace("\n", " "),
                                int(d["created_utc"]),
                                snippet
                            ])
                            seen_ids.add(d["id"])
                if data:
                    oldest_seen = int(data[-1]["created_utc"])
                    print(f"⬅ Oldest now {oldest_seen} ({datetime.utcfromtimestamp(oldest_seen)})")
        except Exception as e:
            print(f"⚠ Pushshift request failed: {e}")

    # --- 3. Save + commit if new data ---
    save_rows(new_rows)
    print(f"💾 Saved {len(new_rows)} new posts. Total now: {len(seen_ids)}")

    # --- 4. Commit at ~9 minutes regardless of new data ---
    elapsed = time.time() - start_time
    if not committed and elapsed >= COMMIT_TIME:
        print("⏰ 9 minutes reached → committing CSV artifact.")
        git_push()
        committed = True

    if elapsed >= RUN_LIMIT:
        if not committed:
            git_push()
        print("🛑 10 minute limit reached. Exiting.")
        break

    time.sleep(1)
