# imports
from collections import Counter
from datetime import datetime
from decouple import config
from nltk.corpus import stopwords
import numpy as np
from os import path
import pandas as pd
import praw
from profanity_filter import remove_bad_words
from PIL import Image
import psycopg2
import re
import schedule
from sqlalchemy import create_engine
import time
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator

import matplotlib.pyplot as plt


def job():
    print("Performing job")
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

    # storing submission data in a pandas dataframe
    submissions = {"title": [],
            "subreddit": [],
            "author": [],
            "score": [],
            "id": [],
            "url": [],
            "num_comments": [],
            "created": [],
            "body": []}

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

    df.to_sql('submissions', engine, if_exists='append')
    
    # scrape comments and store them in comments_df
    comments = {"submission_id": [],
                "comment_id": [],
                "comment": []}


    for id in df['id']:
        submission = reddit.submission(id=id)
        submission.comments.replace_more(limit=None)
        for comment in submission.comments.list():
            comments["submission_id"].append(id)
            comments["comment_id"].append(comment.id)
            comments["comment"].append(comment.body)

    comments_df = pd.DataFrame(comments)

    # store comments_df in sql table 
    comments_df.to_sql('comments', engine, if_exists='append', index=False)


# automate script to run at the same time everyday
schedule.every().day.at("09:30").do(job)


while True:
    schedule.run_pending()
    time.sleep(1)
