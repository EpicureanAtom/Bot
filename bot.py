import praw
import csv
import os
import time
import requests

# Reddit auth (PRAW)
reddit = praw.Reddit(
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    username=os.getenv("USERNAME"),
    password=os.getenv("PASSWORD"),
    user_agent=os.getenv("USER_AGENT"),
)

subreddit = reddit.subreddit("ofcoursethatsasub")
csv_file = "subreddit_refs.csv"

# --- Load already saved posts ---
saved_ids = set()
min_timestamp = None  # track oldest post we've saved
if os.path.exists(csv_file):
    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            saved_ids.add(row[0])
            ts = int(float(row[3]))
            if min_timestamp is None or ts < min_timestamp:
                min_timestamp = ts

print(f"Loaded {len(saved_ids)} existing posts")
print(f"Oldest timestamp: {min_timestamp}")

# --- Helper: Save posts to CSV ---
def save_posts(rows):
    write_header = not os.path.exists(csv_file)
    with open(csv_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["id", "title", "url", "created_utc"])
        writer.writerows(rows)

# --- Fetch new posts via Reddit API ---
def fetch_new_posts():
    new_rows = []
    for submission in subreddit.new(limit=50):
        if submission.id not in saved_ids:
            saved_ids.add(submission.id)
            new_rows.append([
                submission.id,
                submission.title,
                submission.url,
                submission.created_utc,
            ])
    if new_rows:
        save_posts(new_rows)
        print(f"Saved {len(new_rows)} new posts (forward)")

# --- Fetch older posts via Pushshift ---
def fetch_older_posts():
    global min_timestamp
    if min_timestamp is None:
        return
    url = (
        f"https://api.pushshift.io/reddit/submission/search/"
        f"?subreddit=ofcoursethatsasub&before={min_timestamp}&size=50&sort=desc"
    )
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()["data"]
    except Exception as e:
        print(f"Pushshift error: {e}")
        return
    if not data:
        print("No older posts found")
        return
    rows = []
    for post in data:
        pid = post["id"]
        if pid not in saved_ids:
            saved_ids.add(pid)
            rows.append([
                pid,
                post.get("title", ""),
                f"https://reddit.com{post.get('permalink', '')}",
                post["created_utc"],
            ])
            if min_timestamp is None or post["created_utc"] < min_timestamp:
                min_timestamp = post["created_utc"]
    if rows:
        save_posts(rows)
        print(f"Saved {len(rows)} older posts (backfill)")

# --- Main loop ---
while True:
    fetch_new_posts()   # get new activity
    fetch_older_posts() # backfill older history
    time.sleep(1)       # wait before next cycle

