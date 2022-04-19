api_key= 'zNfqKZy5uUME0iNjx3CxFpEj3'
api_key_secret= 'gxa5cTOMtu5RawutmLmQvXdLzrx5niPJwNzaYTneA8V3z4BxXJ'
bearer_token= 'AAAAAAAAAAAAAAAAAAAAALjhbQEAAAAAkimdeH4hYugINA566%2B%2FYHz5dIgo%3DVoDWyisF8aWwrdGJ8QGnrCaFom61ZAsSe5n1KdmoWAZgE2fD9N'
access_token= '1514404381348835330-4dQvRrNPiLneElM5TC3CDQBKhkb4Et'
access_token_secret= '4ym06ksk6SXBYsP0o0QnR4mAev68OOWDsJbyz60nmLfpY'

import requests
import json
#from dotenv import load_dotenv
import os
import tweepy
#load_dotenv()

#loading data to dynamodb
import boto3
AWS_ACCESS_KEY_ID = 'AKIATIVABHTJSUFPAD4C'
AWS_SECRET_ACCESS_KEY = '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'
session = boto3.Session(region_name='us-west-1',
                        aws_access_key_id=AWS_ACCESS_KEY_ID, #get_config('AWS_ACCESS_KEY_ID'),
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY) #get_config('AWS_SECRET_ACCESS_KEY'))
ddb = session.resource('dynamodb')
table = ddb.Table('tweetstreamer')
class DynamoStreamListener(tweepy.Stream):
    """ A listener that continuously receives tweets and stores them in a
        DynamoDB database.
    """
    def __init__(self, api, table):
        super(tweepy.Stream, self).__init__()
        self.api = api
        self.table = table

    def on_status(self, status):

        data = status._json

        content = {}
        content['tweet_id'] = data['id']
        content['timestamp'] = int(data['timestamp_ms'])
        content['lang'] = data['lang']
        content['n_retweets'] = data['retweet_count']
        content['hastags'] = [
            x['text'] for x in data['entities']['hashtags'] if x['text']]
        content['user_mentions'] = [
            x['name'] for x in data['entities']['user_mentions'] if x['name']]
        content['urls'] = [x['url'] for x in data['entities']['urls'] if x['url']]
        content['text'] = data['text']
        content['user_id'] = data['user']['id']
        content['user_name'] = data['user']['name']
        content['coordinates'] = data['coordinates']

        print(content['text'] + '\n')

        try:
            self.table.put_item(Item=content)
        except Exception as e:
            print(str(e))

    def on_error(self, status_code):
        print('Encountered error with status code: {}'.format(status_code))
        return True  # Don't kill the stream

    def on_timeout(self):
        print('Timeout...')
        return True  # Don't kill the stream





def authenticate():
    # Authenticate to Twitter API V2. Stored in .env file.
    #bearer_token = os.getenv('BEARERTOKEN')
    client = tweepy.Client(bearer_token=bearer_token)

    return client

if __name__ == '__main__':
    client = authenticate()

    query = 'elonmusk'#'from:elonmusk -is:retweet'
    tweets = client.search_recent_tweets(query=query, tweet_fields=['author_id', 'created_at'], max_results=10)

    for tweet in tweets.data:
        print(tweet.created_at)
        print(tweet.author_id)
        print(tweet.text)
        print("---")


    TRACK = ['#elonmusk']

        # Connect to Twitter streaming API
    auth = tweepy.OAuthHandler(api_key, api_key_secret)#consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    # Instantiate DynamoStreamListener and pass it as argument to the stream
    sapi = tweepy.streaming.Stream(api_key, api_key_secret, access_token_secret, DynamoStreamListener(api, table))
    # Get tweets that match one of the tracked terms
    sapi.filter(track=TRACK)
