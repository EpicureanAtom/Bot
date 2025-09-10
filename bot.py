import os
import praw
import csv
import time
import requests

reddit = praw.Reddit(
    client_id=os.environ["CLIENT_ID"],
    client_secret=os.environ["CLIENT_SECRET"],
    username=os.environ["USERNAME"],
    password=os.environ["PASSWORD"],
    user_agent=os.environ["USER_AGENT"],
)

subreddit_name = "ofcoursethatsasub"
csv_file = "subreddit_refs.csv"

# Load saved posts
saved_ids = set()
oldest_timestamp = None
if os.path.exists(csv_file):
    with open(csv_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for row in reader:
            if not row or len(row) < 4:
                continue
            saved_ids.add(row[0])
            ts = int(row[3])
            if oldest_timestamp is None or ts < oldest_timestamp:
                oldest_timestamp = ts

def extract_refs(text):
    refs = []
    for word in text.split():
        if word.startswith("r/") and len(word) > 2:
            refs.append(word.strip(",.?!"))
    return refs

def save_to_csv(rows):
    existing = {}
    if os.path.exists(csv_file):
        with open(csv_file, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            for row in reader:
                if not row or len(row) < 4:
                    continue
                existing[row[0]] = row
    for row in rows:
        existing[row[0]] = row
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "title", "subreddit_refs", "created_utc"])
        for row in existing.values():
            writer.writerow(row)

def fetch_pushshift(subreddit, before=None, size=100):
    url = "https://api.pushshift.io/reddit/submission/search/"
    params = {"subreddit": subreddit, "size": size}
    if before:
        params["before"] = before
    r = requests.get(url, params=params)
    if r.status_code == 200:
        return r.json().get("data", [])
    return []

start_time = time.time()
runtime = 9 * 60
print("Bot started: fetching older posts incrementally.")

while time.time() - start_time < runtime:
    batch_rows = []
    batch_posts = fetch_pushshift(subreddit_name, before=oldest_timestamp, size=100)

    if not batch_posts:
        print("No posts returned by Pushshift. Waiting 2s and retrying...")
        time.sleep(2)
        continue  # retry until runtime ends

    batch_oldest = oldest_timestamp
    for post in batch_posts:
        post_id = post["id"]
        if post_id in saved_ids:
            post_ts = int(post["created_utc"])
            if batch_oldest is None or post_ts < batch_oldest:
                batch_oldest = post_ts
            continue

        try:
            submission = reddit.submission(id=post_id)
        except Exception as e:
            print(f"Error fetching {post_id}: {e}")
            continue

        refs = extract_refs(submission.title + " " + submission.selftext)
        if refs:
            batch_rows.append([
                post_id,
                submission.title,
                ", ".join(refs),
                int(submission.created_utc),
            ])
            saved_ids.add(post_id)

        post_ts = int(post["created_utc"])
        if batch_oldest is None or post_ts < batch_oldest:
            batch_oldest = post_ts

    if batch_rows:
        save_to_csv(batch_rows)
        print(f"Saved {len(batch_rows)} posts in this batch (total {len(saved_ids)}).")
    else:
        print("No new posts found in this batch.")

    oldest_timestamp = batch_oldest
    time.sleep(1)

print("Run finished.")


