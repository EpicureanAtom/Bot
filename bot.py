import praw
import requests
import csv
import os
import time
import subprocess
from datetime import datetime

# --------------------------
# CONFIG
# --------------------------
CSV_FILE = "subreddit_refs.csv"
RUN_LIMIT = 600    # 10 minutes total per workflow run
CYCLE_TIME = 270   # 4:30 minutes per cycle
COMMIT_TIME = 540  # 9 minutes, force commit
SUBREDDIT_NAME = "ofcoursethatsasub"
IGNORE_SUB = "r/ofcoursethatsasub"
NEW_POST_LIMIT = 500
BACKFILL_BATCH = 100
CONTEXT_CHARS = 100

# --------------------------
# FILE HELPERS
# --------------------------
def save_rows(rows):
    if not rows:
        return
    file_exists = os.path.isfile(CSV_FILE)
    existing_lines = []
    if file_exists:
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            existing_lines = f.readlines()
    header = existing_lines[0] if existing_lines else "id,subreddit,title,body,created_utc,context\n"
    new_lines = [
        ",".join([row[0], row[1], row[2].replace(",", " "), row[3].replace(",", " "),
                  str(row[4]), row[5].replace(",", " ")]) + "\n" for row in rows
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
# REDDIT SETUP
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
# MAIN LOOP
# --------------------------
seen_ids, oldest_seen = load_existing()
start_time = time.time()
committed = False
cycle = 0

print(f"📂 Loaded {len(seen_ids)} existing rows. Oldest seen: {oldest_seen}")

def extract_mentions(text):
    mentions = []
    words = text.split()
    for word in words:
        if word.lower().startswith("r/") and word.lower() != IGNORE_SUB.lower():
            mentions.append(word)
    return mentions

while True:
    cycle += 1
    cycle_start = time.time()
    new_rows = []

    print(f"\n🔄 Cycle {cycle}: Checking posts/comments...")

    # --- 1. Fetch newest posts ---
    for post in subreddit.new(limit=NEW_POST_LIMIT):
        if post.id in seen_ids:
            continue
        mentions = extract_mentions(post.title + " " + (post.selftext or ""))
        if mentions:
            context = (post.title + " " + (post.selftext or ""))[:CONTEXT_CHARS]
            new_rows.append([post.id, f"r/{post.subreddit.display_name}", post.title,
                             post.selftext or "", int(post.created_utc), context])
            seen_ids.add(post.id)
        post.comments.replace_more(limit=0)
        for comment in post.comments.list():
            if comment.id in seen_ids:
                continue
            mentions = extract_mentions(comment.body)
            if mentions:
                context = comment.body[:CONTEXT_CHARS]
                new_rows.append([comment.id, f"r/{post.subreddit.display_name}", "", comment.body,
                                 int(comment.created_utc), context])
                seen_ids.add(comment.id)

    # --- 2. Backfill older posts via Pushshift ---
    if oldest_seen:
        url = f"https://api.pushshift.io/reddit/submission/search/?subreddit={SUBREDDIT_NAME}&before={oldest_seen}&size={BACKFILL_BATCH}&sort=desc"
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                data = r.json().get("data", [])
                for d in data:
                    if d["id"] in seen_ids:
                        continue
                    mentions = extract_mentions(d.get("title", "") + " " + d.get("selftext", ""))
                    if mentions:
                        context = (d.get("title", "") + " " + d.get("selftext", ""))[:CONTEXT_CHARS]
                        new_rows.append([d["id"], f"r/{d['subreddit']}", d.get("title", ""),
                                         d.get("selftext", ""), int(d["created_utc"]), context])
                        seen_ids.add(d["id"])
                if data:
                    oldest_seen = int(data[-1]["created_utc"])
                    print(f"⬅ Oldest now {oldest_seen} ({datetime.utcfromtimestamp(oldest_seen)})")
        except Exception as e:
            print(f"⚠ Pushshift request failed: {e}")

    # --- 3. Save + push artifact ---
    save_rows(new_rows)
    if new_rows:
        print(f"💾 Saved {len(new_rows)} new rows. Total now: {len(seen_ids)}")
    git_push()

    # --- 4. Wait remaining cycle time ---
    elapsed_cycle = time.time() - cycle_start
    remaining = CYCLE_TIME - elapsed_cycle
    if remaining > 0:
        time.sleep(remaining)

    # --- 5. Commit after 9 minutes even if nothing new ---
    elapsed_total = time.time() - start_time
    if not committed and elapsed_total >= COMMIT_TIME:
        print("⏰ 9 minutes reached → committing CSV regardless of new data")
        git_push()
        committed = True

    # --- 6. Stop after 10 minutes ---
    if elapsed_total >= RUN_LIMIT:
        print("🛑 10 minutes reached → exiting")
        break
