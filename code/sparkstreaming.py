import sys
import os
import pandas as pd
from json import loads
packages = "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7"
os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages {0} pyspark-shell".format(packages))
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession

def store_data(sqlContext, value):
  data = str(value)
  if data is not None:
    json = loads(data)
    df = pd.json_normalize(json)
    df.to_csv(sys.argv[1], mode='a', header=False)
    return df

conf = SparkConf().setAppName("HDSD_Proj")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
ssc = StreamingContext(sc, 3)

KAFKA_BROKER = "masternode:9092"
KAFKA_TOPIC = "hdsd"
print("HDSD")
kafkaStream = KafkaUtils.createDirectStream(ssc,[KAFKA_TOPIC],{"metadata.broker.list":KAFKA_BROKER})
lines = kafkaStream.map(lambda value: store_data(sqlContext, value[1]))
lines.pprint()

ssc.start()
ssc.awaitTermination()

