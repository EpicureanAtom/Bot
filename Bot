import os
import praw
import re
import csv
from datetime import datetime

reddit = praw.Reddit(
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    username=os.getenv("USERNAME"),
    password=os.getenv("PASSWORD"),
    user_agent=os.getenv("USER_AGENT"),
)

subreddit = reddit.subreddit("ofcoursethatsasub")
pattern = re.compile(r"(?:/r/|r/)([A-Za-z0-9_]+)")

with open("subreddit_refs.csv", "a", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    for submission in subreddit.stream.submissions(skip_existing=True):
        matches = pattern.findall(submission.title + " " + submission.selftext)
        if matches:
            for match in matches:
                writer.writerow([datetime.utcnow(), submission.id, match])
                print(f"Found r/{match} in post {submission.id}")
