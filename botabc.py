import os
import praw
import csv
import time
from datetime import datetime
import re

# --------------------------
# Config
# --------------------------
CSV_FILE = "subreddit_refs2.csv"
SUBREDDIT_NAME = "ofcoursethatsasub"
SUB_PATTERN = re.compile(r"\br/([A-Za-z0-9_]+)\b")
SLEEP_BETWEEN = 2  # seconds between writes

# --------------------------
# Reddit API
# --------------------------
reddit = praw.Reddit(
    client_id=os.environ["CLIENT_ID"],
    client_secret=os.environ["CLIENT_SECRET"],
    username=os.environ["USERNAME"],
    password=os.environ["PASSWORD"],
    user_agent=os.environ["USER_AGENT"],
)

subreddit = reddit.subreddit(SUBREDDIT_NAME)

# --------------------------
# Load existing CSV
# --------------------------
saved_ids = set()
rows = []

if os.path.exists(CSV_FILE):
    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            saved_ids.add(row["post_id"])
            rows.append(row)

print(f"📂 Loaded {len(saved_ids)} existing posts from {CSV_FILE}")

# --------------------------
# Prepare CSV writer
# --------------------------
write_header = not os.path.exists(CSV_FILE) or os.path.getsize(CSV_FILE) == 0
csv_file = open(CSV_FILE, "a", newline="", encoding="utf-8")
fieldnames = ["post_id", "type", "context", "subreddit", "author", "timestamp"]
writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

if write_header:
    writer.writeheader()

# --------------------------
# Fetch posts oldest → newest
# --------------------------
print("🔄 Fetching submissions (oldest → newest)... this may take a while.")

all_posts = list(subreddit.new(limit=None))
all_posts.reverse()  # oldest → newest

new_count = 0

for d in all_posts:
    if d.id in saved_ids:
        continue

    text = f"{d.title}\n{getattr(d, 'selftext', '')}"
    matches = SUB_PATTERN.findall(text)
    valid = [m for m in matches if m.lower() != SUBREDDIT_NAME.lower()]
    if not valid:
        continue

    context = text[:200].replace("\n", " ")

    writer.writerow({
        "post_id": d.id,
        "type": "post",
        "context": context,
        "subreddit": f"r/{d.subreddit.display_name}",
        "author": str(d.author) if d.author else None,
        "timestamp": int(d.created_utc),
    })
    csv_file.flush()
    saved_ids.add(d.id)
    new_count += 1

    print(f"💾 Saved post {d.id} ({datetime.utcfromtimestamp(int(d.created_utc))})")
    time.sleep(SLEEP_BETWEEN)

csv_file.close()
print(f"✅ Done. Added {new_count} new posts. Total in file: {len(saved_ids)}")
