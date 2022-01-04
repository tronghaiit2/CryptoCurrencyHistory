import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("HDSD_Visual")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

data = spark.read.csv(sys.argv[1], sep=',',inferSchema=True, header=True)
df = data.toPandas()
df.head()

name = df['name'].head(20)
volume_24h = df['quote.USD.volume_24h'].head(20)
percent_change_1h = df['quote.USD.percent_change_1h'].head(20)

fig = plt.figure(figsize=(20, 20))
plt.bar(name[0:10], volume_24h[0:10])

# Show Plot
plt.show()
