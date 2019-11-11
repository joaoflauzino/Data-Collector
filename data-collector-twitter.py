###################################################################################### LIBRARIES #######################################################################################


import tweepy as tw
import pandas as pd
import numpy as np
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from unidecode import unidecode
from wordcloud import WordCloud
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types
import matplotlib.pyplot as plt
import re
import os
import csv
import random
from random import randint
import psycopg2
import sqlalchemy
import time

###################################################################################### ATTRIBUTES #######################################################################################

client = language.LanguageServiceClient()

consumer_key= 'oU2iXia4DvaIuxQdyzDKEUFPk'
consumer_secret= 'H4ZQUKRF65Iy2dlaV1NZfMWECczQOZixhUDcnM9cDEmBbqIGcZ'
access_token= '219108071-dNN6fAoqJLU4fOY5vuCtxeKKV3T1oC7BE2ViskrS'
access_token_secret= '3dwR6zeT2CmC2AtdUo4YidxWiPLPoZvs8i65u73bRuatu'

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

# Define the search term and the date_since date as variables
search_words = word
transformed_tweets = []
original_tweet = []
date = []
score = []
document = []
magnitude = []
times = []

# Download of stopwords

nltk.download("stopwords")
nltk.download('punkt')

stop_words = []

dbschema = dbschema
engine = sqlalchemy.create_engine(information_database)
con = engine.connect()
sql = SQL
result_set = con.execute(sql)

for r in result_set:
    stop_words.append(r)

for i in range(len(stop_words)):
    stop_words[i] = re.sub('[''|!-.:-@\n"%#$¨*+ªº§_=°;<>]', '', stop_words[i][0])
    stop_words[i] =  stop_words[i].upper()
    
#########################################################################################################################################################################################

######################################################################################## FUNCTIONS FOR CLEANING DATA ######################################################################################

def cleaning_data(data):

    # Cleaning data
    data = unidecode(data)
    data = re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", "", data)
    
    # Lower case
    data = data.upper()

    # Tokenization
    word_tokens = word_tokenize(data)

    # Filter stopwords
    filtered_sentence = []

    for w in word_tokens:
        if w not in stop_words:
            filtered_sentence.append(w)

    final_phrase = ""
    for i in filtered_sentence:
        final_phrase += str(i) + " "

    return final_phrase

########################################################################################################################################################################################

################################################################### GETTING DATA FROM TWITTER ##########################################################################################

# Collect tweets

def get_twitter(search_words):

    new_search = search_words + " -filter:retweets" #Filtering retweets

    tweets = tw.Cursor(api.search,q=new_search,lang="pt",since="2019-03-01", until = "2019-03-14").items(10000)

    for tweet in tweets:
        original_tweet.append((tweet.text).upper())
        transformed_tweets.append(cleaning_data(tweet.text))
        date.append(tweet.created_at)

    return original_tweet, transformed_tweets, date 


########################################################################################################################################################################################

################################################################### GETTING SENTIMENT #################################################################################################


# Getting 50 tweets per time for classification of sentiment in google cloud

def break_twitter(n):
	i = 0
	while i < 1000:
		times.append(50+i)
		i = i+n
	return times

# Getting classification of sentiment in google cloud

def sentiment(textos):

    i = 0
    while i < 50:
    
        document = types.Document(content=textos[i],type=enums.Document.Type.PLAIN_TEXT, language = LANGUAGE) 
        sentiment = client.analyze_sentiment(document).document_sentiment
        score.append(sentiment.score)
        magnitude.append(sentiment.magnitude)
        i = i + 1

        return score, magnitude
    
########################################################################################################################################################################################

################################################################### CREATING DATA FRAME #################################################################################################


def get_score(transformed_tweet):

    j = 0
    teste = []
    i = 0
    n = 50
    times = break_twitter(n)
    for i in range(len(times)):
        sentence = transformed_tweet[j:times[i]]
        j = j + 100
        i = i + 1
        score, magnitude = sentiment(sentence)

    return score, magnitude


def create_dataFrame(data, original_tweet, transformed_tweet, score, magnitude):

    df = pd.DataFrame(np.column_stack([date, original_tweet, transformed_tweet, score, magnitude]), columns=['timestamp', 'original_tweet', 'transformed_tweet', 'score', 'magnitude'])
    return df



if __name__ == '__main__':

    original_tweet, transformed_tweet, date = get_twitter(search_words)
    score, magnitude = get_score(transformed_tweet)
    df = create_dataFrame(date, original_tweet, transformed_tweet, score, magnitude)
