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

# Load existing mentions to avoid duplicates
existing_refs = set()
if os.path.isfile(csv_file):
    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header
        for row in reader:
            if len(row) >= 3:
                existing_refs.add((row[1], row[2]))  # (post_id, subreddit)

# Create CSV with headers if missing
if not os.path.isfile(csv_file):
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "post_id", "mentioned_subreddit"])

saved_count = len(existing_refs)
print(f"Already saved: {saved_count} rows")

# Stream new submissions indefinitely
try:
    for submission in subreddit.stream.submissions(skip_existing=True):
        matches = pattern.findall(submission.title + " " + submission.selftext)
        new_entries = []

        for match in matches:
            key = (submission.id, match)
            if key not in existing_refs:
                existing_refs.add(key)
                new_entries.append([datetime.utcnow(), submission.id, match])
                saved_count += 1
                print(f"Saved r/{match} (total saved: {saved_count})")

        if new_entries:
            with open(csv_file, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerows(new_entries)
                f.flush()
except Exception as e:
    print(f"Error occurred: {e}")
