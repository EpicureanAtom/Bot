import os
import praw
import csv

# Set up Reddit API connection using GitHub secrets
reddit = praw.Reddit(
    client_id=os.environ["CLIENT_ID"],
    client_secret=os.environ["CLIENT_SECRET"],
    username=os.environ["USERNAME"],
    password=os.environ["PASSWORD"],
    user_agent=os.environ["USER_AGENT"],
)

# Target subreddit
subreddit = reddit.subreddit("ofcoursethatsasub")

csv_file = "subreddit_refs.csv"

# Load already saved post IDs
saved_ids = set()
if os.path.exists(csv_file):
    with open(csv_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header
        for row in reader:
            if row:
                saved_ids.add(row[0])

# Prepare to save results
new_rows = []

def extract_refs(text):
    """Find subreddit mentions like r/example."""
    refs = []
    for word in text.split():
        if word.startswith("r/") and len(word) > 2:
            refs.append(word.strip(",.?!"))
    return refs

# Fetch 50 newest posts
for submission in subreddit.new(limit=50):
    if submission.id not in saved_ids:
        refs = extract_refs(submission.title + " " + submission.selftext)
        if refs:
            new_rows.append([submission.id, submission.title, ", ".join(refs)])
            saved_ids.add(submission.id)

# Fetch 50 older posts (skipping ones we already saved)
older_count = 0
for submission in subreddit.new(limit=200):  # scan deeper
    if older_count >= 50:
        break
    if submission.id not in saved_ids:
        refs = extract_refs(submission.title + " " + submission.selftext)
        if refs:
            new_rows.append([submission.id, submission.title, ", ".join(refs)])
            saved_ids.add(submission.id)
            older_count += 1

# Save all data back into CSV (deduplicated)
all_rows = []

# Reload old rows
if os.path.exists(csv_file):
    with open(csv_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for row in reader:
            if row:
                all_rows.append(row)

# Append new rows
all_rows.extend(new_rows)

# Deduplicate by post ID
unique = {}
for row in all_rows:
    unique[row[0]] = row

# Write back to CSV
with open(csv_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["id", "title", "subreddit_refs"])
    for row in unique.values():
        writer.writerow(row)

print(f"Saved {len(new_rows)} new posts. Total now: {len(unique)}")

