import requests
import json
from dotenv import load_dotenv
import os
import tweepy

load_dotenv()

def authenticate():
    # Authenticate to Twitter API V2. Stored in .env file.
    bearer_token = os.getenv('BEARERTOKEN')
    client = tweepy.Client(bearer_token=bearer_token)
    
    return client
        
if __name__ == '__main__':
    client = authenticate()
    
    query = 'from:elonmusk -is:retweet'
    tweets = client.search_recent_tweets(query=query, tweet_fields=['author_id', 'created_at'], max_results=100)
    
    for tweet in tweets.data:
        print(tweet.created_at)
        print(tweet.author_id)
        print(tweet.text)
        print("---")