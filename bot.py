import praw
import requests
import csv
import os
import time
import subprocess
from datetime import datetime

# --------------------------
# Config
# --------------------------
CSV_FILE = "subreddit_refs.csv"    # main CSV
FRESH_START = False                # True to start a new CSV
RUN_LIMIT = 600                    # max 10 minutes
COMMIT_TIME = 540                  # commit around 9 minutes
CYCLE_LIMIT = 270                  # ~4.5 minutes per cycle
IGNORED_SUBREDDIT = "ofcoursethatsasub"

# --------------------------
# File helpers
# --------------------------
def save_rows(rows, csv_file=CSV_FILE):
    """Insert new rows at the top of CSV, newest first."""
    if not rows:
        return

    file_exists = os.path.isfile(csv_file)
    existing_lines = []

    if file_exists:
        with open(csv_file, "r", encoding="utf-8") as f:
            existing_lines = f.readlines()

    header = existing_lines[0] if existing_lines else "id,subreddit,title,created_utc,context,source\n"
    new_lines = [
        ",".join([
            row[0],
            row[1],
            row[2].replace(",", " "),
            str(row[3]),
            row[4].replace(",", " "),
            row[5]
        ]) + "\n" for row in rows
    ]

    with open(csv_file, "w", encoding="utf-8") as f:
        f.write(header)
        f.writelines(new_lines)
        f.writelines(existing_lines[1:] if existing_lines else [])

def load_existing(csv_file=CSV_FILE):
    if not os.path.exists(csv_file) or FRESH_START:
        return set(), None
    ids = set()
    oldest = None
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header
        for row in reader:
            ids.add(row[0])
            try:
                ts = int(row[3])
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

subreddit = reddit.subreddit(IGNORED_SUBREDDIT)

# --------------------------
# Utility: find subreddit mentions
# --------------------------
def find_subreddit_mentions(text):
    mentions = []
    if not text:
        return mentions
    parts = text.split()
    for p in parts:
        if p.startswith("r/") and p[2:].lower() != IGNORED_SUBREDDIT.lower():
            mentions.append(p)
    return mentions

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

    print(f"\n🔄 Cycle {cycle}: Checking for new posts...")
    # --- New posts ---
    for post in subreddit.new(limit=500):
        if post.id in seen_ids:
            continue
        combined_text = post.title + " " + (post.selftext or "")
        mentions = find_subreddit_mentions(combined_text)
        # check comments
        try:
            post.comments.replace_more(limit=0)
            for comment in post.comments.list():
                mentions.extend(find_subreddit_mentions(comment.body))
        except:
            pass
        mentions = list(set(mentions))
        if mentions:
            for m in mentions:
                new_rows.append([
                    post.id,
                    f"r/{post.subreddit.display_name}",
                    post.title.replace("\n"," "),
                    int(post.created_utc),
                    f"Mentions {m}",
                    "post/title/comments"
                ])
            seen_ids.add(post.id)

    # --- Backfill older posts via Pushshift ---
    if oldest_seen:
        print("📉 Backfilling older posts...")
        url = f"https://api.pushshift.io/reddit/submission/search/?subreddit={IGNORED_SUBREDDIT}&before={oldest_seen}&size=100&sort=desc"
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                data = r.json().get("data", [])
                for d in data:
                    if d["id"] in seen_ids:
                        continue
                    combined_text = d.get("title","") + " " + d.get("selftext","")
                    mentions = find_subreddit_mentions(combined_text)
                    if mentions:
                        for m in mentions:
                            new_rows.append([
                                d["id"],
                                f"r/{d['subreddit']}",
                                d["title"].replace("\n"," "),
                                int(d["created_utc"]),
                                f"Mentions {m}",
                                "backfill"
                            ])
                        seen_ids.add(d["id"])
                if data:
                    oldest_seen = int(data[-1]["created_utc"])
                    print(f"⬅ Oldest now {oldest_seen} ({datetime.utcfromtimestamp(oldest_seen)})")
        except Exception as e:
            print(f"⚠ Pushshift request failed: {e}")

    # --- Save + commit ---
    save_rows(new_rows)
    print(f"💾 Saved {len(new_rows)} new posts. Total: {len(seen_ids)}")
    
    elapsed_total = time.time() - start_time
    if not committed and elapsed_total >= COMMIT_TIME:
        print("⏰ 9 minutes reached → committing final artifact")
        git_push()
        committed = True

    # --- Wait for cycle duration (~4.5 minutes) ---
    elapsed_cycle = time.time() - cycle_start
    if elapsed_cycle < CYCLE_LIMIT:
        time.sleep(CYCLE_LIMIT - elapsed_cycle)

    # --- Stop after max run ---
    if elapsed_total >= RUN_LIMIT:
        if not committed:
            git_push()
        print("🛑 10 minutes reached. Exiting.")
        break
