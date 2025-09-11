import praw
import requests
import csv
import os
import time
import subprocess
from datetime import datetime

# --------------------------
# Configuration
# --------------------------
CSV_FILE = "subreddit_refs.csv"    # Change to start fresh
SUBREDDIT_NAME = "ofcoursethatsasub"
RUN_LIMIT = 600       # total runtime in seconds (10 min)
COMMIT_TIME = 540     # 9 min, when CSV is committed
CYCLE_DURATION = 269  # 4 min 29 sec per cycle
BACKFILL_SIZE = 100   # posts per batch for Pushshift

# --------------------------
# Helpers
# --------------------------
def extract_mentions_with_context(text):
    mentions = []
    snippets = []
    for word in text.split():
        if word.startswith("r/") and word.lower() != "r/ofcoursethatsasub":
            mentions.append(word)
            start = max(0, text.find(word)-20)
            end = min(len(text), text.find(word)+20)
            snippets.append(text[start:end].replace("\n"," "))
    return mentions, snippets

def save_rows(rows):
    if not rows:
        return
    file_exists = os.path.isfile(CSV_FILE)
    
    existing_lines = []
    if file_exists:
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            existing_lines = f.readlines()
    
    header = existing_lines[0] if existing_lines else "id,type,subreddit,context,created_utc,mentions\n"
    new_lines = [",".join([row[0], row[1], row[2], row[3].replace(",", " "), str(row[4]), " ".join(row[5])]) + "\n" for row in rows]
    
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
            if len(row) < 6:
                continue
            ids.add(row[0])
            try:
                ts = int(row[4])
                if oldest is None or ts < oldest:
                    oldest = ts
            except:
                continue
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
seen_ids, oldest_seen = load_existing()
start_time = time.time()
committed = False
cycle = 0

print(f"📂 Loaded {len(seen_ids)} existing rows. Oldest seen: {oldest_seen}")

while True:
    cycle += 1
    cycle_start = time.time()
    new_rows = []

    # --- 1. Fetch new submissions ---
    print(f"\n🔄 Cycle {cycle}: Fetching newest posts...")
    try:
        for post in subreddit.new(limit=500):
            if post.id in seen_ids:
                continue
            content = (post.title or "") + " " + (post.selftext or "")
            mentions, snippets = extract_mentions_with_context(content)
            if mentions:
                new_rows.append([post.id, "submission", f"r/{post.subreddit.display_name}", " | ".join(snippets), int(post.created_utc), mentions])
                seen_ids.add(post.id)
    except Exception as e:
        print(f"⚠ Error fetching new submissions: {e}")

    # --- 2. Fetch new comments ---
    try:
        for comment in subreddit.stream.comments(skip_existing=True):
            if comment.id in seen_ids:
                continue
            mentions, snippets = extract_mentions_with_context(comment.body or "")
            if mentions:
                new_rows.append([comment.id, "comment", f"r/{comment.subreddit.display_name}", " | ".join(snippets), int(comment.created_utc), mentions])
                seen_ids.add(comment.id)
    except Exception as e:
        print(f"⚠ Error fetching comments: {e}")

    # --- 3. Backfill older posts via Pushshift ---
    if oldest_seen:
        print("📉 Backfilling older posts...")
        url = f"https://api.pushshift.io/reddit/submission/search/?subreddit={SUBREDDIT_NAME}&before={oldest_seen}&size={BACKFILL_SIZE}&sort=desc"
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json().get("data", [])
            for d in data:
                if d["id"] in seen_ids:
                    continue
                content = (d.get("title") or "") + " " + (d.get("selftext") or "")
                mentions, snippets = extract_mentions_with_context(content)
                if mentions:
                    new_rows.append([d["id"], "submission", f"r/{d['subreddit']}", " | ".join(snippets), int(d["created_utc"]), mentions])
                    seen_ids.add(d["id"])
            if data:
                oldest_seen = int(data[-1]["created_utc"])
        except Exception as e:
            print(f"⚠ Pushshift request failed: {e}")

    # --- 4. Save rows ---
    save_rows(new_rows)
    print(f"💾 Cycle {cycle} saved {len(new_rows)} new rows. Total: {len(seen_ids)}")
    git_push()

    # --- 5. Commit at 9 minutes ---
    elapsed = time.time() - start_time
    if not committed and elapsed >= COMMIT_TIME:
        print("⏰ 9 minutes reached → committing CSV")
        git_push()
        committed = True

    # --- 6. Wait until cycle duration is complete ---
    cycle_elapsed = time.time() - cycle_start
    if cycle_elapsed < CYCLE_DURATION:
        time.sleep(CYCLE_DURATION - cycle_elapsed)
