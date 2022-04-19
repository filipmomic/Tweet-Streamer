import requests
import json
from dotenv import load_dotenv
import os
import tweepy
import config
import boto3

load_dotenv()

def get_secret(secretName):
    # Authenticate to Twitter API V2. Stored in .env file.
    return os.getenv(secretName)
        
class TweetListener(tweepy.StreamingClient):
    #Inherits from tweepy.StreamingClient class
    
    def __init__(self, target_table, max_tweets, *args, **kwargs):
        # Initialize super class - tweepy.StreamingClient 
        super().__init__(*args, **kwargs)
        # Initialize sub class - TweetListener
        self.max_tweets = max_tweets
        self.tweets_count = 0
        self.table = target_table
    
    def on_connect(self):
        # Method called when connected
        print("Listening for tweets..")
        
    def on_data(self, raw_data):
        # Method called when tweet is returned by stream
        self.tweets_count += 1
        json_data = json.loads(raw_data)
        
        #print(json_data) #testing
        
        content = {}
        content['tweet_id'] = int(json_data['data']['id']) #table key
        content['created_at'] = json_data['data']['created_at']
        content['text'] = json_data['data']['text']

        #print(content['text'] + '\n') #testing
        
        try:
            self.table.put_item(Item=content)
            print(f"Wrote {self.tweets_count} tweets to dynamo successfully!")
        except Exception as e:
            print(str(e))
        
        #limit streaming during development/testing
        if self.tweets_count == self.max_tweets:
            print("Closing the connection.. reached max_tweets limit: ", self.max_tweets)
            self.disconnect()
    
    def on_errors(self, errors):
        # Method called when error produced by stream
        print("Something gone wrong: ", str(errors))
        
    def on_response(self, response):
        print("Recieved response:", str(response))

def connect_target_table(tableName):
    aws_access_id = get_secret('ACCESSKEYID')
    aws_secret_access_key = get_secret('SECRETKEY')
    
    session = boto3.Session(region_name='us-west-1',
                        aws_access_key_id=aws_access_id,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    ddb = session.resource('dynamodb')
    table = ddb.Table(tableName)
    
    return table
 
def create_stream_rules(objList, lang):
    # Create a list of tweepy.StreamRules objects to pass into StreamingClient
    
    streamRuleList = []
    for obj in objList:
        ruleStr = obj + ' lang:' + lang
        streamRuleList.append(tweepy.StreamRule(value=ruleStr))
        
    return streamRuleList   

if __name__ == '__main__':
    #db connection
    dynamo_table = connect_target_table('tweetstreamer')
    
    secret = get_secret('BEARERTOKEN')
    client = TweetListener(bearer_token=secret, target_table=dynamo_table, max_retries=3, max_tweets=5)
    
    # Add stream rules to reduce listening events (tweets)
    streamRules = create_stream_rules(config.objects, config.language)
    client.add_rules(add=streamRules)
    
    client.filter(tweet_fields=['created_at'])
    