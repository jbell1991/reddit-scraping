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
    startTime = time.time()

    # connecting to reddit API
    reddit = praw.Reddit(
        client_id=config("CLIENT_ID"),
        client_secret=config("SECRET"),
        user_agent=config("USER"),
        username=config("USERNAME"),
        password=config("PASSWORD")
    )

    subreddit = reddit.subreddit("wallstreetbets")

    hot_wsb = subreddit.hot(limit=150)

    # storing submission data in a dictionary
    submissions = {
        "title": [],
        "subreddit": [],
        "submission_author": [],
        "submission_score": [],
        "submission_id": [],
        "url": [],
        "num_comments": [],
        "submission_created": [],
        "submission_body": []
    }

    # iterate over each submission and store data in the submissions dictionary 
    for submission in hot_wsb:
        submissions["title"].append(submission.title)
        submissions["subreddit"].append(submission.subreddit)
        submissions["submission_author"].append(submission.author)
        submissions["submission_score"].append(submission.score)
        submissions["submission_id"].append(submission.id)
        submissions["url"].append(submission.url)
        submissions["num_comments"].append(submission.num_comments)
        submissions["submission_created"].append(submission.created)
        submissions["submission_body"].append(submission.selftext)

    # transform the submissions dictionary into a pandas dataframe
    df = pd.DataFrame(submissions)

    # convert created to date 
    df['submission_created'] = pd.to_datetime(df['submission_created'], unit='s')

    # convert subreddit column to string
    df['subreddit'] = df['subreddit'].astype(str)

    # convert author column to string
    df['submission_author'] = df['submission_author'].astype(str)

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
        "comment_score": [],
        "comment_author": [],
        "comment_created": [],
        "comment_body": []
    }

    # iterating over each submission and collecting relevent comment data
    for id in df['submission_id']:
        submission = reddit.submission(id=id)
        submission.comments.replace_more(limit=None)
        for comment in submission.comments.list():
            comments["submission_id"].append(id)
            comments["comment_id"].append(comment.id)
            comments["comment_score"].append(comment.score)
            comments["comment_author"].append(comment.author)
            comments["comment_created"].append(comment.created)
            comments["comment_body"].append(comment.body)

    # converting comments dictionary to a pandas dataframe
    comments_df = pd.DataFrame(comments)

    # convert created to date
    comments_df["comment_created"] = pd.to_datetime(comments_df["comment_created"], unit='s')

    # convert author to string
    comments_df["comment_author"] = comments_df["comment_author"].astype(str)

    # store comments_df in sql table
    comments_df.to_sql('comments', engine, if_exists='append', index=False)

    # calculate time it takes for script to run
    executionTime = (time.time() - startTime)
    print('Execution time in minutes: ' + str(executionTime/60))

# automate script to run at the same time everyday
schedule.every().day.at("09:07").do(job)


while True:
    schedule.run_pending()
    time.sleep(1)
