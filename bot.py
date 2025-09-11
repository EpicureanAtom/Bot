import os
import re
import csv
import time
import praw
import subprocess

CSV_FILE = "subreddit_refs.csv"
FRESH_START = True    # set to True to overwrite and start from scratch
CYCLE_TIME = 300      # each cycle = 5 minutes
RUN_TIME = 1800       # full run = 30 minutes


def load_existing():
    """Load existing data and ensure CSV structure has 6 columns."""
    seen_ids = set()
    rows = []

    if os.path.exists(CSV_FILE) and not FRESH_START:
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)

            for row in reader:
                # pad missing columns
                while len(row) < 6:
                    row.append("")
                seen_ids.add(row[0])
                rows.append(row)

    return seen_ids, rows


def save_csv(rows):
    """Save current rows to CSV and commit/push changes."""
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["post_id", "type", "context", "subreddit", "author", "timestamp"]
        )
        writer.writerows(rows)

    # Commit to GitHub
    subprocess.run(["git", "add", CSV_FILE])
    subprocess.run(["git", "commit", "-m", "Cycle update [auto]"], check=False)
    subprocess.run(["git", "pull", "--rebase"], check=False)  # avoid rejected push
    subprocess.run(["git", "push", "origin", "main"], check=False)


def extract_subreddit_mentions(text):
    """Find subreddit mentions like r/example."""
    return set(re.findall(r"r/[A-Za-z0-9_]+", text))


def run_bot():
    reddit = praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent="bot:v1.0 (by u/yourusername)",
    )

    seen_ids, rows = load_existing()
    start_time = time.time()

    while time.time() - start_time < RUN_TIME:
        cycle_start = time.time()
        subreddit = reddit.subreddit("OfCourseThatsASub")

        # scan new posts
        for post in subreddit.new(limit=100):
            if post.id in seen_ids:
                continue

            mentions = extract_subreddit_mentions(
                (post.title or "") + " " + (post.selftext or "")
            )

            # skip if only r/ofcoursethatsasub is mentioned
            mentions = {m for m in mentions if m.lower() != "r/ofcoursethatsasub"}
            if not mentions:
                continue

            for m in mentions:
                rows.append(
                    [
                        post.id,
                        "post",
                        (post.title or post.selftext)[:80],
                        m,
                        str(post.author),
                        int(post.created_utc),
                    ]
                )

            seen_ids.add(post.id)

            # check comments too
            post.comments.replace_more(limit=0)
            for c in post.comments.list():
                cid = f"{post.id}_{c.id}"
                if cid in seen_ids:
                    continue

                c_mentions = extract_subreddit_mentions(c.body)
                c_mentions = {m for m in c_mentions if m.lower() != "r/ofcoursethatsasub"}
                if not c_mentions:
                    continue

                for m in c_mentions:
                    rows.append(
                        [
                            cid,
                            "comment",
                            c.body[:80],
                            m,
                            str(c.author),
                            int(c.created_utc),
                        ]
                    )

                seen_ids.add(cid)

        # save & push after each cycle
        save_csv(rows)

        elapsed = time.time() - cycle_start
        if elapsed < CYCLE_TIME:
            time.sleep(CYCLE_TIME - elapsed)

    # final save
    save_csv(rows)


if __name__ == "__main__":
    run_bot()
