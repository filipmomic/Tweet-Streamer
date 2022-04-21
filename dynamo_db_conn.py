import os
from dotenv import load_dotenv
import boto3

load_dotenv()

def connect_dynamo_table(tableName):
    aws_access_id = os.getenv('ACCESSKEYID')
    aws_secret_access_key = os.getenv('SECRETKEY')
    
    session = boto3.Session(region_name='us-west-1',
                        aws_access_key_id=aws_access_id,
                        aws_secret_access_key=aws_secret_access_key)
    ddb = session.resource('dynamodb')
    table = ddb.Table(tableName)
    
    return table