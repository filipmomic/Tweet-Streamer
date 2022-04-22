
import os
from dotenv import load_dotenv
import boto3
import dynamo_db_conn as dyn_db
import pandas as pd
from dynamodb_json import json_util as json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from sqlalchemy import create_engine

#db connection
dynamo_table = dyn_db.connect_dynamo_table('tweetstreamer')

#get all results from dynamo table
response = dynamo_table.scan() #Scan has 1 MB limit on the amount of data it will return in a request, so we need to paginate through the results in a loop.
data = response['Items']

while 'LastEvaluatedKey' in response:
    response = dynamo_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    data.extend(response['Items'])

#process them into a dataframe (from json)
df = pd.DataFrame(json.loads(data))


#run sentiment analysis
s = SentimentIntensityAnalyzer()
df["sentiment_scores"] = df["text"].apply(lambda x: x.replace('@','')).apply(lambda x: s.polarity_scores(x)['compound']) #for compopund, 1 = positive, -1 = negative sentiment

#write dataframe to AWS RDS: Postgresql
host = 'twitter-cars-stream-1.cu24gjcelr46.us-west-1.rds.amazonaws.com'
port = '5432'
username = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
db = os.getenv("POSTGRES_DB")

conn_string = f'postgres://{username}:{password}@{host}:{port}/{db}'
engine = create_engine(conn_string)
df.to_sql('twitter_cars_sentiment', conn_string, index=False, if_exists='replace')

#delete out all data from dynamodb that is older than last run to keep db small in size


# connect tablaeu on live connection to AWS RDS and visualize
