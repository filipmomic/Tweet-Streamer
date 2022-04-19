import requests
import json
from dotenv import load_dotenv
import os
import tweepy
import config
import boto3
AWS_ACCESS_KEY_ID = 'AKIATIVABHTJSUFPAD4C'
AWS_SECRET_ACCESS_KEY = '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'
api_key= 'zNfqKZy5uUME0iNjx3CxFpEj3'
api_key_secret= 'gxa5cTOMtu5RawutmLmQvXdLzrx5niPJwNzaYTneA8V3z4BxXJ'
bearer_token= 'AAAAAAAAAAAAAAAAAAAAALjhbQEAAAAAkimdeH4hYugINA566%2B%2FYHz5dIgo%3DVoDWyisF8aWwrdGJ8QGnrCaFom61ZAsSe5n1KdmoWAZgE2fD9N'
access_token= '1514404381348835330-4dQvRrNPiLneElM5TC3CDQBKhkb4Et'
access_token_secret= '4ym06ksk6SXBYsP0o0QnR4mAev68OOWDsJbyz60nmLfpY'
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
        #print(raw_data)

        #DynamoDB functions########
        data = raw_data._json
        print("data is", data)
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

        print("this is the contents:", content['text'] + '\n')

        try:
            self.table.put_item(Item=content)
        except Exception as e:
            print(str(e))


        if self.tweets_count == self.max_tweets:
            print("Closing the connection.. reached max_tweets limit: ", self.max_tweets)
            self.disconnect()

    def on_errors(self, errors):
        # Method called when error produced by stream
        print("Something gone wrong: ", str(errors))

    def on_response(self, response):
        print("Recieved response:", str(response))

    #def on_error(self, status_code):
     #   print('Encountered error with status code: {}'.format(status_code))
      #  return True  # Don't kill the stream

    def on_timeout(self):
        print('Timeout...')
        return True  # Don't kill the stream

def create_stream_rules(objList, lang):
    # Create a list of tweepy.StreamRules objects to pass into StreamingClient

    streamRuleList = []
    for obj in objList:
        ruleStr = obj + ' lang:' + lang
        streamRuleList.append(tweepy.StreamRule(value=ruleStr))

    return streamRuleList

if __name__ == '__main__':
    secret = get_secret('BEARERTOKEN')
    session = boto3.Session(region_name='us-west-1',
                            aws_access_key_id=AWS_ACCESS_KEY_ID, #get_config('AWS_ACCESS_KEY_ID'),
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY) #get_config('AWS_SECRET_ACCESS_KEY'))
    ddb = session.resource('dynamodb')
    client = TweetListener(bearer_token=secret, max_retries=3, max_tweets=20)   #, table = ddb.Table('tweetstreamer') )

    # Add stream rules to reduce listening events (tweets)
    streamRules = create_stream_rules(config.objects, config.language)
    client.add_rules(add=streamRules)

    client.filter(tweet_fields=['created_at'])

    #client.on_status
