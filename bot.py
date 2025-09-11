import praw
import requests
import csv
import os
import time
import subprocess
import re
from datetime import datetime

# --------------------------
# CONFIG
# --------------------------
CSV_FILE = "subreddit_reffs.csv"
RUN_LIMIT = 600    # 10 minutes max
COMMIT_TIME = 540  # commit at ~9 minutes
CONTEXT_CHARS = 20  # chars before/after subreddit mention
SUBREDDIT_NAME = "ofcoursethatsasub"
IGNORE_SUB = "r/ofcoursethatsasub"
BACKFILL_SIZE = 100

# Set to True if you want to start with a fresh CSV
START_FRESH = False

# --------------------------
# Helper functions
# --------------------------
def extract_mentions_with_context(text):
    mentions = re.findall(r"r/\w+", text)
    mentions = [m for m in mentions if m.lower() != IGNORE_SUB.lower()]
    context_snippets = []
    for m in mentions:
        idx = text.lower().find(m.lower())
        if idx != -1:
            start = max(0, idx - CONTEXT_CHARS)
            end = min(len(text), idx + len(m) + CONTEXT_CHARS)
            snippet = text[start:end].replace("\n", " ")
            context_snippets.append(snippet)
    return mentions, context_snippets

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
    
    header = "id,type,subreddit,title_or_body,created_utc,mentioned_subs\n"
    if existing_lines and not START_FRESH:
        header = existing_lines[0]

    new_lines = []
    for row in rows:
        new_lines.append(",".join([
            row[0],
            row[1],
            row[2],
            row[3].replace(",", " "),
            str(row[4]),
            "|".join(row[5])
        ]) + "\n")

    with open(CSV_FILE, "w", encoding="utf-8") as f:
        f.write(header)
        f.writelines(new_lines)
        if existing_lines and not START_FRESH:
            f.writelines(existing_lines[1:])

def load_existing():
    if START_FRESH or not os.path.exists(CSV_FILE):
        return set(), None
    ids = set()
    oldest = None
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if not row or len(row) < 5:
                continue
            ids.add(row[0])
            try:
                ts = int(row[4])
                if oldest is None or ts < oldest:
                    oldest = ts
            except ValueError:
                continue
    return ids, oldest

def git_push():
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions[bot]"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions[bot]@users.noreply.github.com"], check=True)
        subprocess.run(["git", "add", CSV_FILE], check=True)
        subprocess.run(["git", "commit", "-m", "Update subreddit_reffs.csv [auto]"], check=True)
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
    new_rows = []

    # --- 1. Fetch new posts via Reddit API ---
    print(f"\n🔄 Cycle {cycle}: Checking for new posts...")
    for post in subreddit.new(limit=500):
        if post.id in seen_ids:
            continue
        content = (post.title or "") + " " + (post.selftext or "")
        mentions, snippets = extract_mentions_with_context(content)
        if mentions:
            new_rows.append([post.id, "submission", f"r/{post.subreddit.display_name}", " | ".join(snippets), int(post.created_utc), mentions])
            seen_ids.add(post.id)

    # --- 2. Fetch new comments ---
    print("💬 Checking comments...")
    for comment in subreddit.stream.comments(skip_existing=True):
        if comment.id in seen_ids:
            continue
        mentions, snippets = extract_mentions_with_context(comment.body or "")
        if mentions:
            new_rows.append([comment.id, "comment", f"r/{comment.subreddit.display_name}", " | ".join(snippets), int(comment.created_utc), mentions])
            seen_ids.add(comment.id)

    # --- 3. Backfill older posts via Pushshift ---
    if oldest_seen:
        print("📉 Backfilling older posts...")
        url = f"https://api.pushshift.io/reddit/submission/search/?subreddit={SUBREDDIT_NAME}&before={oldest_seen}&size={BACKFILL_SIZE}&sort=desc"
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
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
                    print(f"⬅ Oldest now {oldest_seen} ({datetime.utcfromtimestamp(oldest_seen)})")
        except Exception as e:
            print(f"⚠ Pushshift request failed: {e}")

    # --- 4. Save + commit ---
    if new_rows:
        save_rows(new_rows)
        print(f"💾 Saved {len(new_rows)} new rows. Total now: {len(seen_ids)}")
        git_push()
    else:
        print("✅ No new rows this cycle.")

    # --- 5. Timing & limits ---
    elapsed = time.time() - start_time
    if not committed and elapsed >= COMMIT_TIME:
        print("⏰ 9 minutes reached → committing CSV")
        git_push()
        committed = True
    if elapsed >= RUN_LIMIT:
        if not committed:
            git_push()
        print("🛑 10 minutes limit reached. Exiting.")
        break

    time.sleep(1)
