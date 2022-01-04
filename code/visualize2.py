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

column = ['name', 'quote.USD.percent_change_24h', 'last_updated']
data = spark.read.csv(sys.argv[1], sep=',',inferSchema=True, header=True)
df = data.toPandas()
df.head()

dataLine = df[column]
dataLine = dataLine.to_numpy()
dataLine.shape
percent_change_24h_Bitcoin = []
date_Bitcoin = []
percent_change_24h_Ethereum = []
date_Ethereum = []
for line in dataLine:
    if (line[0] == "Bitcoin"):
        percent_change_24h_Bitcoin.append(line[1])
        day = line[2]
        day = str(day)
        date_Bitcoin.append(day[0:10])
    elif (line[0] == "Ethereum"):
        percent_change_24h_Ethereum.append(line[1])
        day = line[2]
        day = str(day)
        date_Ethereum.append(day[0:10])

# Figure Size
fig = plt.figure(figsize=(30, 20))

plt.plot(date_Bitcoin[0:15], percent_change_24h_Bitcoin[0:15], label='Bitcoin')
plt.plot(date_Ethereum[0:15],
         percent_change_24h_Ethereum[0:15], label='Ethereum')
plt.legend()
# Show Plot
plt.show()