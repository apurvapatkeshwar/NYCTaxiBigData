from numpy import array
from pyspark.mllib.clustering import KMeans, KMeansModel
from math import sqrt
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


distinctValues = df.select('start_station_latitude','start_station_longitude').distinct()
distinctValues.toPandas().to_csv('stations.csv')
groupby_gender=df.groupby('gender').count()
groupby_gender.toPandas().to_csv('gender_analysis.csv')

groupby_station_count=df.groupby(['start_station_latitude','start_station_longitude']).count()
groupby_station_count.toPandas().to_csv('usage_count.csv')


