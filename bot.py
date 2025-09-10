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
USER_AGENT = os.getenv("USER_AGENT", "").strip()  # remove extra spaces

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

# Subreddit to monitor
subreddit = reddit.subreddit("ofcoursethatsasub")

# Regex to find r/subreddit mentions
pattern = re.compile(r"(?:/r/|r/)([A-Za-z0-9_]+)")

# CSV file to store references
csv_file = "subreddit_refs.csv"

# Create CSV with headers if it doesn't exist
if not os.path.isfile(csv_file):
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.write
