# imports
from decouple import config
import pandas as pd
import praw
import psycopg2
import schedule
from sqlalchemy import create_engine
import time


def job():
    current_day = time.strftime("%m/%d/%Y")
    print(f"Performing job on {current_day}")

    # connecting to reddit API
    reddit = praw.Reddit(
        client_id=config("CLIENT_ID"),
        client_secret=config("SECRET"),
        user_agent=config("USER"),
        username=config("USERNAME"),
        password=config("PASSWORD")
    )

    subreddit = reddit.subreddit("wallstreetbets")

    hot_wsb = subreddit.hot(limit=1000)

    # storing submission data in a dictionary
    submissions = {
        "title": [],
        "subreddit": [],
        "author": [],
        "score": [],
        "id": [],
        "url": [],
        "num_comments": [],
        "created": [],
        "body": []
    }

    # iterate over each submission and store data in the submissions dictionary 
    for submission in hot_wsb:
        submissions["title"].append(submission.title)
        submissions["subreddit"].append(submission.subreddit)
        submissions["author"].append(submission.author)
        submissions["score"].append(submission.score)
        submissions["id"].append(submission.id)
        submissions["url"].append(submission.url)
        submissions["num_comments"].append(submission.num_comments)
        submissions["created"].append(submission.created)
        submissions["body"].append(submission.selftext)

    # transform the submissions dictionary into a pandas dataframe
    df = pd.DataFrame(submissions)

    # convert created to date 
    df['created'] = pd.to_datetime(df['created'], unit='s')

    # convert subreddit column to string
    df['subreddit'] = df['subreddit'].astype(str)

    # convert author column to string
    df['author'] = df['author'].astype(str)

    # connect to postgresql database
    db_pass = config("PASSWORD")
    engine = create_engine(
        f'postgresql://postgres:{db_pass}@localhost:5432/postgres')

    # store pandas dataframe in sql database
    df.to_sql('submissions', engine, if_exists='append')

    # create dictionary to store comments
    comments = {
        "submission_id": [],
        "comment_id": [],
        "score": [],
        "author": [],
        "created": [],
        "comment": []
    }

    # iterating over each submission and collecting relevent comment data
    for id in df['id']:
        submission = reddit.submission(id=id)
        submission.comments.replace_more(limit=None)
        for comment in submission.comments.list():
            comments["submission_id"].append(id)
            comments["comment_id"].append(comment.id)
            comments["score"].append(comment.score)
            comments["author"].append(comment.author)
            comments["created"].append(comment.created)
            comments["comment"].append(comment.body)

    # converting comments dictionary to a pandas dataframe
    comments_df = pd.DataFrame(comments)

    # store comments_df in sql table
    comments_df.to_sql('comments', engine, if_exists='append', index=False)


# automate script to run at the same time everyday
schedule.every().day.at("09:30").do(job)


while True:
    schedule.run_pending()
    time.sleep(1)
