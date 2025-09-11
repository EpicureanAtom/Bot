import os
import praw
import csv
import time

# --- Reddit connection ---
reddit = praw.Reddit(
    client_id=os.environ["CLIENT_ID"],
    client_secret=os.environ["CLIENT_SECRET"],
    username=os.environ["USERNAME"],
    password=os.environ["PASSWORD"],
    user_agent=os.environ["USER_AGENT"],
)

subreddit_name = "ofcoursethatsasub"
csv_file = "subreddit_refs.csv"

# --- Load saved posts ---
saved_ids = set()
if os.path.exists(csv_file):
    with open(csv_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for row in reader:
            if not row or len(row) < 4:
                continue
            saved_ids.add(row[0])

# --- Helper functions ---
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

# --- Main loop ---
print("Bot started: monitoring for new posts...")
try:
    subreddit = reddit.subreddit(subreddit_name)
    for submission in subreddit.stream.submissions(skip_existing=True):
        if submission.id in saved_ids:
            continue

        refs = extract_refs(submission.title + " " + submission.selftext)
        if refs:
            row = [submission.id, submission.title, ", ".join(refs), int(submission.created_utc)]
            save_to_csv([row])
            saved_ids.add(submission.id)
            print(f"Saved post {submission.id} with {len(refs)} references.")

        time.sleep(1)  # small pause to avoid rate limits

except KeyboardInterrupt:
    print("Bot stopped manually.")
