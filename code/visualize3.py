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

column = ['name', 'quote.USD.price', 'last_updated']
data = spark.read.csv(sys.argv[1], sep=',',inferSchema=True, header=True)
df = data.toPandas()
df.head()

dataLine = df[column]
dataLine = dataLine.to_numpy()
dataLine.shape
price_Bitcoin = []
date_Bitcoin = []
price_Ethereum = []
date_Ethereum = []
for line in dataLine:
    if (line[0] == "Bitcoin"):
        price_Bitcoin.append(line[1]/100000)
        day = line[2]
        day = str(day)
        date_Bitcoin.append(day[0:10])
    elif (line[0] == "Ethereum"):
        price_Ethereum.append(line[1])
        day = line[2]
        day = str(day)
        date_Ethereum.append(day[0:10])

# Figure Size
fig = plt.figure(figsize=(30, 20))

plt.plot(date_Bitcoin[0:15], price_Bitcoin[0:15], label='Bitcoin')
plt.legend()
# Show Plot
plt.show()