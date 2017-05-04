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
#data=data.filter(lambda row:row.replaceAll('\"',''))
data = data.map(lambda x: x.split(",")).\
    map(lambda y: (float(y[9]),float(y[10])))
#

customSchema = StructType([ \
    StructField("end_station_latitude", DoubleType(), True), \
    StructField("end_station_longitude", DoubleType(), True)])

df = sqlContext.createDataFrame(data, customSchema)
split_col = split(df['starttime'], ' ')
df.show()
df = df.withColumn('start_date', split_col.getItem(0))
df = df.withColumn('start_t', split_col.getItem(1))
df.show()


groupby_station_count=df.groupby(['end_station_latitude','end_station_longitude']).count()
groupby_station_count.toPandas().to_csv('dropoff_count.csv')
