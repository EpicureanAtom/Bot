import praw
import requests
import csv
import os
import time
import subprocess
from datetime import datetime
import re

import subprocess

# Configure Git identity for this runner so subprocess calls work
subprocess.run(["git", "config", "--global", "user.name", "EpicureanAtom"], check=True)
subprocess.run(["git", "config", "--global", "user.email", "miloradovicdragutin@gmail.com"], check=True)


CSV_FILE = "subreddit_refs.csv"
FRESH_START = True       # True for a fresh CSV each run
RUN_LIMIT = 1800         # 30 minutes max
CYCLE_TIME = 260         # ~4:20 min per cycle
COMMIT_TIME = 540        # force commit at ~9 minutes

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
                # Fill missing columns if necessary
                row += [''] * (6 - len(row))
            rows.append(row)
            ids.add(row[0])
            try:
                ts = int(row[5])
                if oldest is None or ts < oldest:
                    oldest = ts
            except ValueError:
                continue
    return rows, ids, oldest

def save_csv(rows):
    """Save CSV and commit/push."""
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["post_id", "type", "context", "subreddit", "author", "timestamp"])
        writer.writerows(rows)

    try:
        subprocess.run(["git", "add", CSV_FILE], check=True)
        subprocess.run(["git", "commit", "-m", "Cycle update [auto]"], check=True)
    except subprocess.CalledProcessError:
        print("⚠ Nothing new to commit, skipping commit.")

    try:
        subprocess.run(["git", "pull", "--rebase", "--autostash"], check=True)
    except subprocess.CalledProcessError:
        print("⚠ Git pull failed, continuing anyway.")

    try:
        subprocess.run(["git", "push", "origin", "main"], check=True)
    except subprocess.CalledProcessError:
        print("⚠ Git push failed, continuing anyway.")

# --------------------------
# Reddit setup
# --------------------------
reddit = praw.Reddit(
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    username=os.getenv("USERNAME"),
    password=os.getenv("PASSWORD"),
    user_agent=os.getenv("USER_AGENT"),
)

subreddit = reddit.subreddit("ofcoursethatsasub")
SUB_PATTERN = re.compile(r"\br/([A-Za-z0-9_]+)\b")

# --------------------------
# Main loop
# --------------------------
rows, seen_ids, oldest_seen = load_existing()
start_time = time.time()
committed = False
cycle = 0

print(f"📂 Loaded {len(seen_ids)} posts. Oldest seen: {oldest_seen}")

while True:
    cycle += 1
    new_rows = []
    print(f"\n🔄 Cycle {cycle}: Scanning posts...")

    for post in subreddit.new(limit=100):
        if post.id in seen_ids:
            continue

        text = f"{post.title}\n{post.selftext or ''}"
        matches = SUB_PATTERN.findall(text)
        valid = [m for m in matches if m.lower() != "ofcoursethatsasub"]

        if valid:
            context = text[:200].replace("\n", " ")
            new_rows.append([post.id, "post", context, f"r/{post.subreddit.display_name}",
                             str(post.author), int(post.created_utc)])
            seen_ids.add(post.id)
            print(f"✅ Found subreddit in post: {post.id}")

        # check comments
        post.comments.replace_more(limit=0)
        for comment in post.comments.list():
            if comment.id in seen_ids:
                continue
            matches = SUB_PATTERN.findall(comment.body)
            valid = [m for m in matches if m.lower() != "ofcoursethatsasub"]
            if valid:
                context = comment.body[:200].replace("\n", " ")
                new_rows.append([comment.id, "comment", context, f"r/{post.subreddit.display_name}",
                                 str(comment.author), int(comment.created_utc)])
                seen_ids.add(comment.id)
                print(f"✅ Found subreddit in comment: {comment.id}")

    # Backfill older posts via Pushshift
    if oldest_seen:
        url = f"https://api.pushshift.io/reddit/submission/search/?subreddit=ofcoursethatsasub&before={oldest_seen}&size=100&sort=desc"
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                data = r.json().get("data", [])
                for d in data:
                    if d["id"] in seen_ids:
                        continue
                    text = f"{d.get('title','')}\n{d.get('selftext','')}"
                    matches = SUB_PATTERN.findall(text)
                    valid = [m for m in matches if m.lower() != "ofcoursethatsasub"]
                    if valid:
                        context = text[:200].replace("\n", " ")
                        new_rows.append([d["id"], "post", context, f"r/{d['subreddit']}",
                                         d.get("author"), int(d["created_utc"])])
                        seen_ids.add(d["id"])
                if data:
                    oldest_seen = int(data[-1]["created_utc"])
                    print(f"⬅ Oldest post now {oldest_seen} ({datetime.utcfromtimestamp(oldest_seen)})")
        except Exception as e:
            print(f"⚠ Pushshift failed: {e}")

    # Save every cycle
    rows = new_rows + rows
    save_csv(rows)
    print(f"💾 Cycle {cycle} finished. New rows: {len(new_rows)}. Total rows: {len(rows)}")

    # Force commit at 9 minutes if not yet done
    elapsed = time.time() - start_time
    if not committed and elapsed >= COMMIT_TIME:
        print("⏰ 9 min reached → force commit")
        save_csv(rows)
        committed = True

    if elapsed >= RUN_LIMIT:
        print("🛑 Time limit reached. Final save + exit.")
        save_csv(rows)
        break

    time.sleep(CYCLE_TIME)
