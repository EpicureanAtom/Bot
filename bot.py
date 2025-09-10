import os
import praw
import csv
import time

# --- Reddit API connection ---
reddit = praw.Reddit(
    client_id=os.environ["CLIENT_ID"],
    client_secret=os.environ["CLIENT_SECRET"],
    username=os.environ["USERNAME"],
    password=os.environ["PASSWORD"],
    user_agent=os.environ["USER_AGENT"],
)

subreddit = reddit.subreddit("ofcoursethatsasub")
csv_file = "subreddit_refs.csv"

# --- Load already saved posts ---
saved_ids = set()
oldest_timestamp = None
if os.path.exists(csv_file):
    with open(csv_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for row in reader:
            if not row or len(row) < 4:  # skip incomplete/broken rows
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

# --- Timed loop (runs ~9 minutes) ---
start_time = time.time()
runtime = 9 * 60  # 9 minutes

print("Bot started. Running for ~9 minutes...")

while time.time() - start_time < runtime:
    new_rows = []

    # Fetch all posts available, then filter older than oldest_timestamp
    if oldest_timestamp is None:
        posts = subreddit.new(limit=None)
    else:
        posts = subreddit.new(limit=None)

    for submission in posts:
        if submission.id in saved_ids:
            continue
        if oldest_timestamp is None or int(submission.created_utc) < oldest_timestamp:
            refs = extract_refs(submission.title + " " + submission.selftext)
            if refs:
                new_rows.append([
                    submission.id,
                    submission.title,
                    ", ".join(refs),
                    int(submission.created_utc),
                ])
                saved_ids.add(submission.id)
                if oldest_timestamp is None or int(submission.created_utc) < oldest_timestamp:
                    oldest_timestamp = int(submission.created_utc)

    if new_rows:
        save_to_csv(new_rows)
        print(f"Cycle complete: saved {len(new_rows)} new posts (total {len(saved_ids)}).")
    else:
        print("Cycle complete: no new posts found.")

    time.sleep(1)  # wait 1 second before next cycle

print("Run finished.")
