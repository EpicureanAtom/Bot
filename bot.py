import os
import praw
import csv
import time
import requests

# --- Reddit API connection for post content/details ---
reddit = praw.Reddit(
    client_id=os.environ["CLIENT_ID"],
    client_secret=os.environ["CLIENT_SECRET"],
    username=os.environ["USERNAME"],
    password=os.environ["PASSWORD"],
    user_agent=os.environ["USER_AGENT"],
)

subreddit_name = "ofcoursethatsasub"
csv_file = "subreddit_refs.csv"

# --- Load already saved posts ---
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

# --- Helper functions ---
def extract_refs(text):
    """Find subreddit mentions like r/example."""
    refs = []
    for word in text.split():
        if word.startswith("r/") and len(word) > 2:
            refs.append(word.strip(",.?!"))
    return refs

def save_to_csv(rows):
    """Save rows to CSV, keeping old data."""
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

def fetch_pushshift(subreddit, before=None, size=500):
    """Fetch posts from Pushshift API."""
    url = "https://api.pushshift.io/reddit/submission/search/"
    params = {
        "subreddit": subreddit,
        "size": size,
    }
    if before:
        params["before"] = before
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()["data"]
    return []

# --- Timed loop (runs ~9 minutes) ---
start_time = time.time()
runtime = 9 * 60  # 9 minutes

print("Bot started. Running for ~9 minutes using Pushshift...")

while time.time() - start_time < runtime:
    new_rows = []

    # Fetch posts
    pushshift_posts = fetch_pushshift(subreddit_name, before=oldest_timestamp, size=500)
    if not pushshift_posts:
        print("No more posts available from Pushshift.")
        break

    for post in pushshift_posts:
        post_id = post["id"]
        if post_id in saved_ids:
            continue

        try:
            submission = reddit.submission(id=post_id)
        except Exception as e:
            print(f"Error fetching {post_id} from Reddit: {e}")
            continue

        refs = extract_refs(submission.title + " " + submission.selftext)
        if refs:
            new_rows.append([
                post_id,
                submission.title,
                ", ".join(refs),
                int(submission.created_utc),
            ])
            saved_ids.add(post_id)
            if oldest_timestamp is None or int(submission.created_utc) < oldest_timestamp:
                oldest_timestamp = int(submission.created_utc)

    if new_rows:
        save_to_csv(new_rows)
        print(f"Cycle complete: saved {len(new_rows)} new posts (total {len(saved_ids)}).")
    else:
        print("Cycle complete: no new posts found.")

    time.sleep(1)  # 1-second wait between cycles

print("Run finished.")
