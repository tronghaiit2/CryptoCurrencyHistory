from kafka import KafkaConsumer
import pandas as pd
from json import loads

consumer = KafkaConsumer(bootstrap_servers='masternode:9092')
consumer.subscribe('hdsd')
for message in consumer:
    print(message)
    message = message.value
    json = loads(message)
    df = pd.json_normalize(json)
    df.to_csv("bigdata.csv", mode='a', header=False)

