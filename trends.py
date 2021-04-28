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
        client_id = config("CLIENT_ID"),
        client_secret = config("SECRET"),
        user_agent = config("USER"),
        username = config("USERNAME"),
        password = config("PASSWORD")
    )

    subreddit = reddit.subreddit("wallstreetbets")

    hot_wsb = subreddit.hot(limit=1000)

    # storing data in a pandas dataframe
    dict = {"title": [],
            "subreddit": [],
            "score": [],
            "id": [],
            "url": [],
            "comms_num": [],
            "created": [],
            "body": []}

    for submission in hot_wsb:
        dict["title"].append(submission.title)
        dict['subreddit'].append(submission.subreddit)
        dict["score"].append(submission.score)
        dict["id"].append(submission.id)
        dict["url"].append(submission.url)
        dict["comms_num"].append(submission.num_comments)
        dict["created"].append(submission.created)
        dict["body"].append(submission.selftext)
        
    df = pd.DataFrame(dict)

    # function that cleans the text in the submission
    def clean_submission(text):
        text = text.lower()
        text = ' '.join(
            re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t'])|(\w+:\/\/\S+)", " ", text).split())
        return text

    # applying clean submission function to the title and body columns
    df['title'] = df['title'].apply(lambda x: clean_submission(x))
    df['body'] = df['body'].apply(lambda x: clean_submission(x))

    body_text = " ".join(body for body in df.body)
    # combining title and body text
    title_text = " ".join(title for title in df.title) + body_text

    # set stop words/letters
    # stopwords = set(STOPWORDS)
    # stopwords.add("I'm, It's, s, m")

    # remove stopwords
    stop = stopwords.words('english')

    # Exclude stopwords with Python's list comprehension and pandas.DataFrame.apply.
    df['title'] = df['title'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop)]))
    df['body'] = df['body'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop)]))

    # print(df.shape)
    # df.head()

    # frequency for title
    title_freq = Counter(" ".join(df['title']).split()).most_common(30)
    title_freq = pd.DataFrame(title_freq, columns=['Word', 'Frequency'])
    # add current date column
    title_freq["date"] = time.strftime("%m/%d/%Y")
    # drop index
    title_freq = title_freq.set_index('Word')

    # connect to postgresql database
    db_pass = config("PASSWORD")
    engine = create_engine(f'postgresql://postgres:{db_pass}@localhost:5432/postgres')

    title_freq.to_sql('title_freq', engine, if_exists='append')

# automate script to run at the same time everyday
schedule.every().day.at("09:30").do(job)


while True:
    schedule.run_pending()
    time.sleep(1)
