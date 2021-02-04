from decouple import config
import numpy as np
from os import path
import pandas as pd
import praw
from PIL import Image
import re
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator

import matplotlib.pyplot as plt


reddit = praw.Reddit(
    client_id = config("CLIENT_ID"),
    client_secret = config("SECRET"),
    user_agent = config("USER"),
    username = config("USERNAME"),
    password = config("PASSWORD")
)

subreddit = reddit.subreddit("wallstreetbets")

hot_wsb = subreddit.hot(limit=1000)

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
    # print(submission.title)
    # print(submission.score)
    # print(submission.id)
    # print(submission.url)
    # comments = submission.comments
    # for comment in comments:
    #     print(20*'-')
    #     print('COMMENT', comment.body)
    #     if len(comment.replies) > 0:
    #         for reply in comment.replies:
    #             print(f'REPLY: {reply.body}')

df = pd.DataFrame(dict)

# could do some NLP - word cloud, sentiment analysis

# function that cleans the text in the submission
def clean_submission(text):
    text = text.lower()
    text = ' '.join(
        re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t'])|(\w+:\/\/\S+)", " ", text).split())
    return text
    

# applying clean submission function to the title and body columns
df['title'] = df['title'].apply(lambda x: clean_submission(x))
df['body'] = df['body'].apply(lambda x: clean_submission(x))

print(df.shape)
print(df.head(50))

body_text = " ".join(body for body in df.body)
title_text = " ".join(title for title in df.title) + body_text

stopwords = set(STOPWORDS)
stopwords.add("I'm, It's, s, m")

mask = np.array(Image.open("wallstreetbets3.png"))
print(mask)

wc = WordCloud(background_color="white", max_words=2000, mask=mask,
               stopwords=stopwords, max_font_size=20, random_state=42)

# generate word cloud
wc.generate(title_text)

# create coloring from image
image_colors=ImageColorGenerator(mask)

# show
fig, axes=plt.subplots(1, 3)
axes[0].imshow(wc, interpolation = "bilinear")
# recolor wordcloud and show
# we could also give color_func=image_colors directly in the constructor
axes[1].imshow(wc.recolor(
    color_func=image_colors), interpolation = "bilinear")
axes[2].imshow(mask, cmap = plt.cm.gray,
interpolation = "bilinear")
for ax in axes:
    ax.set_axis_off()
plt.show()
