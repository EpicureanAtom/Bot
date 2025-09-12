import os
import csv
import time
import requests
import re
from datetime import datetime

# --------------------------
# Config
# --------------------------
CSV_FILE = "subreddit_refs2.csv"
SUBREDDIT_NAME = "ofcoursethatsasub"
CHUNK_SIZE = 100
SLEEP_BETWEEN_CHUNKS = 2
MAX_RETRIES = 5

SUB_PATTERN = re.compile(r"\br/([A-Za-z0-9_]+)\b", re.IGNORECASE)

# --------------------------
# Helpers
# --------------------------
def load_existing():
    rows = []
    saved_ids = set()
    newest_ts = 0
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
                saved_ids.add(row["post_id"] + "_" + row.get("sub_ref", ""))
                ts = int(row.get("timestamp", 0))
                if ts > newest_ts:
                    newest_ts = ts
    return rows, saved_ids, newest_ts

def save_rows(rows):
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["post_id", "type", "context", "subreddit", "author", "timestamp", "sub_ref"])
        writer.writerows(rows)

def fetch_pushshift(subreddit, after, size=CHUNK_SIZE):
    url = (
        f"https://api.pushshift.io/reddit/submission/search/"
        f"?subreddit={subreddit}&after={after}&size={size}&sort=asc"
    )
    retries = 0
    while retries < MAX_RETRIES:
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                return r.json().get("data", [])
            else:
                print(f"⚠ Bad response {r.status_code}, retrying...")
        except Exception as e:
            print(f"⚠ Pushshift request failed: {e}")
        retries += 1
        time.sleep(2 ** retries)
    return []

def fetch_comments(post_id, after=None):
    """Recursively fetch all comments for a post."""
    comments = []
    batch_size = 100
    after_param = after or 0

    while True:
        url = (
            f"https://api.pushshift.io/reddit/comment/search/"
            f"?link_id={post_id}&after={after_param}&size={batch_size}&sort=asc"
        )
        retries = 0
        while retries < MAX_RETRIES:
            try:
                r = requests.get(url, timeout=30)
                if r.status_code == 200:
                    data = r.json().get("data", [])
                    comments.extend(data)
                    break
                else:
                    print(f"⚠ Comment fetch bad response {r.status_code}")
            except Exception as e:
                print(f"⚠ Comment fetch error: {e}")
            retries += 1
            time.sleep(2 ** retries)
        if not data or len(data) < batch_size:
            break
        after_param = int(data[-1]["created_utc"])
    return comments

def find_sub_refs(text):
    matches = SUB_PATTERN.findall(text or "")
    return [m for m in matches if m.lower() != SUBREDDIT_NAME.lower()]

# --------------------------
# Main
# --------------------------
rows, saved_ids, newest_ts = load_existing()
if newest_ts == 0:
    print("📂 No existing CSV, starting from the beginning...")
else:
    print(f"📂 Resuming from timestamp {newest_ts} ({datetime.utcfromtimestamp(newest_ts)})")

all_new = 0

while True:
    posts = fetch_pushshift(SUBREDDIT_NAME, newest_ts)
    if not posts:
        print("✅ No more posts found. Backfill complete.")
        break

    new_rows = []
    for post in posts:
        post_id = post["id"]
        post_text = f"{post.get('title','')}\n{post.get('selftext','')}"
        post_refs = find_sub_refs(post_text)

        # Add post references
        for ref in post_refs:
            key = post_id + "_" + ref.lower()
            if key not in saved_ids:
                context = post_text[:200].replace("\n", " ")
                new_rows.append([
                    post_id,
                    "post",
                    context,
                    f"r/{SUBREDDIT_NAME}",
                    post.get("author"),
                    int(post["created_utc"]),
                    ref
                ])
                saved_ids.add(key)
                all_new += 1

        # Fetch all comments
        comments = fetch_comments(post_id)
        for comment in comments:
            comment_refs = find_sub_refs(comment.get("body",""))
            for ref in comment_refs:
                key = post_id + "_" + ref.lower()
                if key not in saved_ids:
                    context = comment.get("body","")[:200].replace("\n"," ")
                    new_rows.append([
                        post_id,
                        "comment",
                        context,
                        f"r/{SUBREDDIT_NAME}",
                        comment.get("author"),
                        int(comment["created_utc"]),
                        ref
                    ])
                    saved_ids.add(key)
                    all_new += 1

    if new_rows:
        rows.extend(new_rows)
        save_rows(rows)
        print(f"💾 Saved {len(new_rows)} new rows. Total collected: {len(saved_ids)}")
    else:
        print("⚠ No new subreddit mentions found in this chunk.")

    newest_ts = int(posts[-1]["created_utc"])
    time.sleep(SLEEP_BETWEEN_CHUNKS)

print(f"✅ Finished. Total new references added: {all_new}")
