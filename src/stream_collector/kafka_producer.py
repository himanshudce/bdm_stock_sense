# Note -
# to run this,  start kafka server and zookeeper before

import pandas as pd
import tweepy
from kafka import KafkaProducer
import logging
# from config import tweet_config
import time



# set twitter API Keys (user specific)

# 1. tweet keys (user specific)
api_key = 'YnexwDPzFJqX61H9BBOQvEUBx'
api_key_secret = 'lUE47pTi98oadCkfoiednY2B9P1QwH2YsZamwJ7jMiywNfl6tu'
access_token = '1181621632768462848-Q3Q37PBA4ohatLGSJwX0BpMs8cQjII'
access_token_secret = 'TGYmhkg1u7YMgLUPat0CpwqYKVLgpJLs0YKsiCiw8gcRq'

# 2. Setting up the tweet config
# api_key = tweet_config.api_key
# api_key_secret = tweet_config.api_key_secret
# access_token = tweet_config.access_token
# access_token_secret = tweet_config.access_token_secret


# create kafka Producer
# one broker that will respond to a Metadata API Request, kafka is running at 9092 which will respond to the tweet stream api 
# ref - https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
producer = KafkaProducer(bootstrap_servers='localhost:9092')


# kafka topic name
topic_name = 'twitter'


# Define TweetGetter class which inherits tweepy stream class
# ref - https://docs.tweepy.org/pl/latest/stream.html
class TweetGetter(tweepy.Stream):
    
    """# if we want to use our specific socket, we can pass it in and send data to this socket
    def __init__(self,socket):
        self.client_socket = socket"""
    
    
    # on data is function which contains raw data from Stream class
    def on_data(self, raw_data):
        print(raw_data)
        # send data from API to kafka broker/server (socket-9092)
        producer.send(topic_name, value=raw_data)
        return True

    def on_error(self, status_code):
        print(status_code)
        if status_code == 420:
            # returning False in on_data disconnects the stream
            print("error: disconnected from stream")
            return False
    
    def on_limit(self,status):
        print ("Twitter API Rate Limit")
        print("Waiting...")
        time.sleep(15 * 60) # wait 15 minutes and try again
        print("Resuming")
        return True


    # to filter out tweets if needed (nothing to filter for now) 
    def start_streaming_tweets(self, search_term):
        self.filter(track=search_term, stall_warnings=True, languages=["en"])


if __name__ == '__main__':

    # get all the companies (Queries according to )
    companies = pd.read_csv("top_25NSE.csv")
    companies_list = list(companies["Company Name"].values)

    
    # create stream class
    twitter_stream = TweetGetter(api_key, api_key_secret, access_token, access_token_secret)
    

    # search tearm to specify if needed or if we want to filter tweets based on keywords or locations
    search_term = companies_list
    twitter_stream.start_streaming_tweets(search_term)


    # # to start the stream wihtout specific keywords
    # twitter_stream.sample()