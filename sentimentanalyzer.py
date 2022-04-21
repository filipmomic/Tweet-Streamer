import os
from dotenv import load_dotenv
import boto3
import dynamo_db_conn as dyn_db

#db connection
dynamo_table = dyn_db.connect_dynamo_table('tweetstreamer')

#get all results from dynamo table


#process them into a dataframe (from json)



#run sentiment analysis



#collect statisitcs on number of tweets for each rule/tag broken up by positive/negative sentiment



#write dataframe with statisitcs to AWS RDS: Postgresql



#delete out all data from dynamodb that is older than last run to keep db small in size


# connect tablaeu on live connection to AWS RDS and visualize 