import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps,loads
import json, boto3
from kafka import KafkaConsumer
import requests

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], 
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

pd.set_option('display.max_colwidth', None)
x=requests.get('https://api.covid19api.com/summary').json()


global_dict=x['Global']
global_dict.pop('Date')
for c in x['Countries']:
    c.pop('ID')
    c.pop('Premium')
    producer.send('kafka-topic',value={**global_dict,**c})
                         
                         
