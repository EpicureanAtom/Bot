import os
import praw
import csv
import time

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

def extract_refs(text):
    """Find subreddit mentions like r/example."""
    refs = []
    for word in text.split():
        if word.startswith("r/") and len(word) > 2:
            refs.append(word.strip(",.?!"))
    return refs

def save_to_csv(new_rows):
    """Append new rows to the CSV, deduplicating by post ID."""
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

start_time = time.time()
batch_size = 50
batch_count = 0

print("Starting bot, will run until GitHub timeout (~10 min).")

# Keep processing until GitHub kills the job (~600s)
while time.time() - start_time < 550:  # ~9 minutes
    new_rows = []
    processed = 0

    for submission in subreddit.new(limit=200):
        if processed >= batch_size:
            break
        if submission.id not in saved_ids:
            refs = extract_refs(submission.title + " " + submission.selftext)
            if refs:
                new_rows.append([submission.id, submission.title, ", ".join(refs)])
                saved_ids.add(submission.id)
                processed += 1

    if new_rows:
        save_to_csv(new_rows)
        batch_count += 1
        print(f"Batch {batch_count}: saved {len(new_rows)} posts (total saved: {len(saved_ids)})")
    else:
        print("No new posts found, waiting...")
        time.sleep(30)  # wait before checking again

print("Finished run.")
