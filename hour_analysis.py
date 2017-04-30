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
    map(lambda y: (int(y[0]), y[1], y[2],int(y[3]),y[4],float(y[5]),float(y[6]),int(y[7]),y[8],float(y[9]),float(y[10]),int(y[11]),y[12],int(y[14])))
#

customSchema = StructType([ \
    StructField("tripduration", IntegerType(), True), \
    StructField("starttime", StringType(), True), \
    StructField("stoptime", StringType(), True), \
    StructField("start_station_id", IntegerType(), True), \
    StructField("start_station_name", StringType(), True), \
    StructField("start_station_latitude", DoubleType(), True), \
    StructField("start_station_longitude", DoubleType(), True), \
    StructField("end_station_id", IntegerType(), True), \
    StructField("end_station_name", StringType(), True), \
    StructField("end_station_latitude", DoubleType(), True), \
    StructField("end_station_longitude", DoubleType(), True), \
    StructField("bikeid", IntegerType(), True), \
    StructField("usertype", StringType(), True), \
    StructField("gender", IntegerType(), True)])

df = sqlContext.createDataFrame(data, customSchema)
split_col = split(df['starttime'], ' ')
df = df.withColumn('start_date', split_col.getItem(0))
df = df.withColumn('start_t', split_col.getItem(1))
groupby_date=df.groupby('start_date').count().sort(col("start_date").asc())
groupby_date.toPandas().to_csv('mycsv.csv')

