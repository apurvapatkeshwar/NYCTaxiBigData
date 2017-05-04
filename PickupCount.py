from numpy import array
from math import sqrt
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import split
from pyspark.sql.functions import col

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
data1 = sc.textFile("citibikedata/201601-citibike-tripdata.csv")
data2 = sc.textFile("citibikedata/201611-citibike-tripdata.csv")
data=sc.textFile("citibikedata")
header = data.first()
header2 = data2.first()
data = data.filter(lambda row : row != header and row != header2)
data = data.map(lambda row : row.replace('"',''))
data = data.map(lambda x: x.split(",")).\
    map(lambda y: (y[1],float(y[5]),float(y[6])))
#

customSchema = StructType([ \
    StructField("starttime", StringType(), True), \
    StructField("start_station_latitude", DoubleType(), True), \
    StructField("start_station_longitude", DoubleType(), True)])

df = sqlContext.createDataFrame(data, customSchema)
split_col = split(df['starttime'], ' ')
df.show()
df = df.withColumn('start_date', split_col.getItem(0))
df = df.withColumn('start_t', split_col.getItem(1))


df.show()
split_col1 = split(df['start_date'], "/")
df = df.withColumn('start_month', split_col1.getItem(0))
df = df.withColumn('start_d', split_col1.getItem(1))

#df = df.withColumn('start_year', split_col.getItem(2))

#df=df.drop('start_date').collect()
df.show()
split_col2 = split(df['start_t'], ':')
df = df.withColumn('start_hour', split_col2.getItem(0))
df = df.withColumn('start_minute', split_col2.getItem(1))

groupby_station_count=df.groupby(['start_station_latitude','start_station_longitude']).count()
groupby_station_count.toPandas().to_csv('pickup_count.csv')
