import praw
import requests
import csv
import os
import time
import subprocess
from datetime import datetime

CSV_FILE = "subreddit_refs.csv"
RUN_LIMIT = 600    # 10 minutes
COMMIT_TIME = 540  # commit at ~9 minutes
CYCLE_LIMIT = 500  # number of posts per cycle
IGNORE_SUB = "ofcoursethatsasub"

def load_existing():
    """Load existing CSV, fix rows with missing columns, and get set of seen IDs and oldest timestamp."""
    if not os.path.exists(CSV_FILE):
        return set(), None

    ids = set()
    oldest = None
    fixed_lines = []

    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None or len(header) < 6:
            header = ["id", "subreddit", "title", "created_utc", "context_snippet", "extra"]
        fixed_lines.append(header)

        for row in reader:
            while len(row) < 6:
                row.append("")
            ids.add(row[0])
            try:
                ts = int(row[3])
                if oldest is None or ts < oldest:
                    oldest = ts
            except ValueError:
                pass
            fixed_lines.append(row)

    # Overwrite CSV with corrected rows
    with open(CSV_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(fixed_lines)

    return ids, oldest

def save_rows(rows):
    """Insert new rows at the top of the CSV so newest posts appear first."""
    if not rows:
        return
    file_exists = os.path.isfile(CSV_FILE)
    
    # Load existing CSV
    existing_lines = []
    if file_exists:
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            existing_lines = f.readlines()
    
    # Prepare new CSV content
    header = existing_lines[0] if existing_lines else "id,subreddit,title,created_utc,context_snippet,extra\n"
    new_lines = [",".join([row[0], row[1], row[2].replace(",", " "), str(row[3]), row[4].replace(",", " "), row[5]]) + "\n" for row in rows]
    
    # Write new content at the top
    with open(CSV_FILE, "w", encoding="utf-8") as f:
        f.write(header)
        f.writelines(new_lines)
        f.writelines(existing_lines[1:] if existing_lines else [])

def git_push():
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions[bot]"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions[bot]@users.noreply.github.com"], check=True)
        subprocess.run(["git", "add", CSV_FILE], check=True)
        subprocess.run(["git", "commit", "-m", "Update subreddit_refs.csv [auto]"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✔ CSV pushed to GitHub")
    except subprocess.CalledProcessError:
        print("⚠ Nothing new to commit")

# --------------------------
# Reddit setup
# --------------------------
reddit = praw.Reddit(
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    username=os.getenv("USERNAME"),
    password=os.getenv("PASSWORD"),
    user_agent=os.getenv("USER_AGENT")
)

subreddit = reddit.subreddit(IGNORE_SUB)

# --------------------------
# Helper functions
# --------------------------
def extract_mentions(text):
    """Return subreddit mentions excluding the ignored one."""
    mentions = []
    words = text.split()
    for w in words:
        if w.lower().startswith("r/") and w[2:].lower() != IGNORE_SUB.lower():
            mentions.append(w)
    return mentions

def get_context_snippet(text, mention):
    """Return a snippet around the subreddit mention."""
    idx = text.lower().find(mention.lower())
    if idx == -1:
        return ""
    start = max(idx - 30, 0)
    end = min(idx + len(mention) + 30, len(text))
    return text[start:end]

# --------------------------
# Main loop
# --------------------------
seen_ids, oldest_seen = load_existing()
start_time = time.time()
committed = False
cycle = 0

print(f"📂 Loaded {len(seen_ids)} existing rows. Oldest seen: {oldest_seen}")

while True:
    cycle += 1
    new_rows = []

    print(f"\n🔄 Cycle {cycle}: Checking for new posts...")

    # Fetch newest posts
    for post in subreddit.new(limit=CYCLE_LIMIT):
        if post.id in seen_ids:
            continue
        mentions = extract_mentions(post.title) + extract_mentions(getattr(post, "selftext", ""))

        # Check comments
        try:
            post.comments.replace_more(limit=0)
            for comment in post.comments.list():
                mentions += extract_mentions(comment.body)
        except Exception:
            pass

        if mentions:
            for mention in mentions:
                snippet = get_context_snippet(post.title + " " + getattr(post, "selftext", ""), mention)
                new_rows.append([
                    post.id,
                    f"r/{post.subreddit.display_name}",
                    post.title.replace("\n", " "),
                    int(post.created_utc),
                    snippet.replace("\n", " "),
                    mention
                ])
            seen_ids.add(post.id)

    # Backfill older posts via Pushshift
    if oldest_seen:
        print("📉 Backfilling older posts...")
        url = f"https://api.pushshift.io/reddit/submission/search/?subreddit={IGNORE_SUB}&before={oldest_seen}&size=100&sort=desc"
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                data = r.json().get("data", [])
                for d in data:
                    if d["id"] in seen_ids:
                        continue
                    mentions = extract_mentions(d.get("title", "")) + extract_mentions(d.get("selftext", ""))
                    if mentions:
                        for mention in mentions:
                            snippet = get_context_snippet(d.get("title", "") + " " + d.get("selftext", ""), mention)
                            new_rows.append([
                                d["id"],
                                f"r/{d['subreddit']}",
                                d.get("title", "").replace("\n", " "),
                                int(d.get("created_utc")),
                                snippet.replace("\n", " "),
                                mention
                            ])
                        seen_ids.add(d["id"])
                if data:
                    oldest_seen = int(data[-1]["created_utc"])
                    print(f"⬅ Oldest now {oldest_seen} ({datetime.utcfromtimestamp(oldest_seen)})")
        except Exception as e:
            print(f"⚠ Pushshift request failed: {e}")

    # Save + commit
    if new_rows:
        save_rows(new_rows)
        print(f"💾 Saved {len(new_rows)} new posts. Total now: {len(seen_ids)}")
        git_push()
    else:
        print("✅ No new posts this cycle.")

    # Timing & limits
    elapsed = time.time() - start_time
    if not committed and elapsed >= COMMIT_TIME:
        print("⏰ 9 minutes reached → final commit before stopping.")
        git_push()
        committed = True
    if elapsed >= RUN_LIMIT:
        if not committed:
            git_push()
        print("🛑 10 minute limit reached. Exiting.")
        break

    time.sleep(1)
