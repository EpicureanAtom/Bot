import os
import praw
import re
import csv
from datetime import datetime

# Read environment variables (GitHub secrets)
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
USER_AGENT = os.getenv("USER_AGENT", "").strip()

# Validate secrets
if not all([CLIENT_ID, CLIENT_SECRET, USERNAME, PASSWORD, USER_AGENT]):
    raise ValueError("One or more Reddit secrets are missing!")

# Connect to Reddit
reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    username=USERNAME,
    password=PASSWORD,
    user_agent=USER_AGENT
)

subreddit = reddit.subreddit("ofcoursethatsasub")
pattern = re.compile(r"(?:/r/|r/)([A-Za-z0-9_]+)")
csv_file = "subreddit_refs.csv"

# Create CSV with header if missing
if not os.path.isfile(csv_file):
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "post_id", "mentioned_subreddit"])

# Count previously saved entries
with open(csv_file, newline="", encoding="utf-8") as f:
    saved_count = sum(1 for row in f) - 1  # subtract header

print(f"Already saved: {saved_count} rows")

# Append new matches
try:
    for submission in subreddit.new(limit=50):
        matches = pattern.findall(submission.title + " " + submission.selftext)
        if matches:
            with open(csv_file, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                for match in matches:
                    writer.writerow([datetime.utcnow(), submission.id, match])
                    f.flush()
                    saved_count += 1
                    print(f"Saved r/{match} (total saved: {saved_count})")
except Exception as e:
    print(f"Error occurred: {e}")

- name: Commit results
  run: |
    git config --global user.name "github-actions[bot]"
    git config --global user.email "github-actions[bot]@users.noreply.github.com"
    git add subreddit_refs.csv
    git commit -m "Update subreddit references [skip ci]" || echo "No changes to commit"
    git push

