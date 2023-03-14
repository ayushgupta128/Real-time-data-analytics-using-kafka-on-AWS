import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import loads
import json
from kafka import KafkaConsumer
import boto3

# Creating Consumer APIhui
consumer = KafkaConsumer(
    'demo-testing',
     bootstrap_servers=['localhost:9092'], #add your IP here
    value_deserializer=lambda x: loads(x.decode('utf-8')))

#Uploading realtime data on S3
s3=boto3.client('s3',aws_access_key_id=<access_key>,aws_secret_access_key=<secret_access_key>)
for c in consumer:
    df1=pd.DataFrame(loads(list(c.value.values())[0]),index=[0])
    df1.to_csv('file1.csv',index=False)
    message=c.value
    s3.upload_file('file1.csv',Bucket='kafka-buckets',Key='confirmed-cases/day_id={0}/cases.csv'.format(list(c.value.keys())[0]))
    print(c.value)    
