import requests
import json
from dotenv import load_dotenv
import os
import tweepy
import config

load_dotenv()

def get_secret(secretName):
    # Authenticate to Twitter API V2. Stored in .env file.
    return os.getenv(secretName)
        
class TweetListener(tweepy.StreamingClient):
    #Inherits from tweepy.StreamingClient class
    
    def __init__(self, max_tweets, *args, **kwargs):
        # Initialize super class - tweepy.StreamingClient 
        super().__init__(*args, **kwargs)
        # Initialize sub class - TweetListener
        self.max_tweets = max_tweets
        self.tweets_count = 0
    
    def on_connect(self):
        # Method called when connected
        print("Listening for tweets..")
        
    def on_data(self, raw_data):
        # Method called when data is returned by stream
        self.tweets_count += 1
        print(raw_data)
        
        if self.tweets_count == self.max_tweets:
            print("Closing the connection.. reached max_tweets limit: ", self.max_tweets)
            self.disconnect()
    
    def on_errors(self, errors):
        # Method called when error produced by stream
        print("Something gone wrong: ", str(errors))
        
    def on_response(self, response):
        print("Recieved response:", str(response))

def create_stream_rules(objList, lang):
    # Create a list of tweepy.StreamRules objects to pass into StreamingClient
    
    streamRuleList = []
    for obj in objList:
        ruleStr = obj + ' lang:' + lang
        streamRuleList.append(tweepy.StreamRule(value=ruleStr))
        
    return streamRuleList

if __name__ == '__main__':
    secret = get_secret('BEARERTOKEN')
    client = TweetListener(bearer_token=secret, max_retries=3, max_tweets=20)
    
    # Add stream rules to reduce listening events (tweets)
    streamRules = create_stream_rules(config.objects, config.language)
    client.add_rules(add=streamRules)
    
    client.filter(tweet_fields=['created_at'])
    