import pandas as pd

# load in bad words
df = pd.read_csv('bad_single.csv', usecols=[0], names=None)

# convert to list
bad_words = df['Bad_words'].to_list()


def return_bad_words(text):
    """Takes in plain text without punctuation, separated by spaces
    and returns text with profanity removed"""
    text = text.split()
    for word in bad_words:
        while word in text:
            # remove the word from text
            text.remove(word)
    return " ".join(text)
