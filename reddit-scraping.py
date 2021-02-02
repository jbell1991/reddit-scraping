from decouple import config
import praw


reddit = praw.Reddit(
    client_id = config("CLIENT_ID"),
    client_secret = config("SECRET"),
    user_agent = config("USER"),
    username = config("USERNAME"),
    password = config("PASSWORD")
)

subreddit = reddit.subreddit("wallstreetbets")

hot_wsb = subreddit.hot(limit=1)

for submission in hot_wsb:
    print(submission.title)
    print(submission.score)
    print(submission.id)
    print(submission.url)