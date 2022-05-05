import tweepy
from tweepy.streaming import Stream
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import twitter_credentials
import json
import re
import time

topic_name = "elon"

class StdOutListener(tweepy.Stream):
    def on_data(self, data):
        # try:
        tweet_data = json.loads(data)
        tweet = tweet_data['text']
        if 'RT' not in tweet[:5]:
            tweet = tweet_data['text']
            tweet = clean_tweet(tweet)
            print(tweet)
            producer.send(topic_name, tweet.encode('utf-8'))

        return True

    def on_error(self, status):
        print (status)


def clean_tweet(tweet):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())


producer = KafkaProducer(bootstrap_servers='localhost:9092')
#print(twitter_credentials.CONSUMER_KEY)
l = StdOutListener(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET, twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
# stream = Stream(auth, l)
l.filter(languages=["en"], track=["iphone"])

# # Creating the authentication object
# auth = tweepy.OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
# # Setting your access token and secret
# auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
# # Creating the API object by passing in auth information
# api = tweepy.API(auth) 

# producer = KafkaProducer(bootstrap_servers='localhost:9092')

# def get_twitter_data():
#     res = api.search_tweets("Apple OR iphone OR iPhone")
#     for i in res:
#         producer.send(topic_name, i)

# get_twitter_data()


