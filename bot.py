import praw
import requests
import csv
import os
import time
import subprocess
from datetime import datetime

CSV_FILE = "subreddit_refs.csv"
FRESH_START = False  # Set True to start with a fresh CSV
RUN_LIMIT = 600      # 10 minutes total runtime
COMMIT_TIME = 540    # Commit after ~9 minutes
CONTEXT_LENGTH = 100 # Number of characters in context snippet

# --------------------------
# File helpers
# --------------------------
def save_rows(rows):
    """Insert new rows at the top of the CSV so newest posts appear first."""
    if not rows:
        return
    file_exists = os.path.isfile(CSV_FILE)
    
    existing_lines = []
    if file_exists:
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            existing_lines = f.readlines()
    
    header = existing_lines[0] if existing_lines else "id,subreddit,title,created_utc,author,context\n"
    new_lines = [
        ",".join([
            row[0],
            row[1],
            row[2].replace(",", " "),
            str(row[3]),
            row[4].replace(",", " "),
            row[5].replace(",", " ")
        ]) + "\n"
        for row in rows
    ]
    
    with open(CSV_FILE, "w", encoding="utf-8") as f:
        f.write(header)
        f.writelines(new_lines)
        f.writelines(existing_lines[1:] if existing_lines else [])

def load_existing():
    if FRESH_START or not os.path.exists(CSV_FILE):
        return set(), None
    ids = set()
    oldest = None
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header
        for row in reader:
            if not row or len(row) < 6:
                continue
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

SUBREDDIT_NAME = "ofcoursethatsasub"
subreddit = reddit.subreddit(SUBREDDIT_NAME)

# --------------------------
# Helper to find subreddit mentions in text
# --------------------------
def find_other_subreddits(text):
    mentions = []
    for part in text.split():
        if part.lower().startswith("r/") and part.lower() != f"r/{SUBREDDIT_NAME}".lower():
            mentions.append(part)
    return mentions

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

        print(f"\n🔄 Cycle {cycle}: Checking for new posts...")

        # --- 1. Fetch new posts ---
        for post in subreddit.new(limit=500):
            if post.id in seen_ids:
                continue
            mentions = find_other_subreddits(post.title + " " + post.selftext)
            if mentions:
                context = (post.title + " " + post.selftext)[:CONTEXT_LENGTH]
                new_rows.append([
                    post.id,
                    f"r/{post.subreddit.display_name}",
                    post.title,
                    int(post.created_utc),
                    str(post.author),
                    context
                ])
                seen_ids.add(post.id)
            # --- check comments ---
            post.comments.replace_more(limit=0)
            for comment in post.comments.list():
                mentions_c = find_other_subreddits(comment.body)
                if mentions_c and comment.id not in seen_ids:
                    context = comment.body[:CONTEXT_LENGTH]
                    new_rows.append([
                        comment.id,
                        f"r/{post.subreddit.display_name}",
                        f"Comment on {post.id}",
                        int(comment.created_utc),
                        str(comment.author),
                        context
                    ])
                    seen_ids.add(comment.id)

        # --- 2. Backfill older posts via Pushshift ---
        if oldest_seen:
            url = f"https://api.pushshift.io/reddit/submission/search/?subreddit={SUBREDDIT_NAME}&before={oldest_seen}&size=100&sort=desc"
            try:
                r = requests.get(url, timeout=30)
                if r.status_code == 200:
                    data = r.json().get("data", [])
                    for d in data:
                        if d["id"] not in seen_ids:
                            mentions = find_other_subreddits(d.get("title", "") + " " + d.get("selftext", ""))
                            if mentions:
                                context = (d.get("title", "") + " " + d.get("selftext", ""))[:CONTEXT_LENGTH]
                                new_rows.append([
                                    d["id"],
                                    f"r/{d['subreddit']}",
                                    d.get("title", ""),
                                    int(d["created_utc"]),
                                    d.get("author", ""),
                                    context
                                ])
                                seen_ids.add(d["id"])
                    if data:
                        oldest_seen = int(data[-1]["created_utc"])
                        print(f"⬅ Oldest now {oldest_seen} ({datetime.utcfromtimestamp(oldest_seen)})")
            except Exception as e:
                print(f"⚠ Pushshift request failed: {e}")

        # --- 3. Save + commit ---
        save_rows(new_rows)
        if new_rows:
            print(f"💾 Saved {len(new_rows)} new items. Total now: {len(seen_ids)}")
        else:
            print("✅ No new items this cycle.")

        elapsed = time.time() - start_time
        if not committed and elapsed >= COMMIT_TIME:
            print("⏰ 9 minutes reached → committing CSV.")
            git_push()
            committed = True
        if elapsed >= RUN_LIMIT:
            if not committed:
                git_push()
            print("🛑 10 minutes limit reached. Exiting.")
            break

        time.sleep(1)

except KeyboardInterrupt:
    print("🛑 Interrupted by user. Saving CSV...")
    save_rows(new_rows)
    git_push()
