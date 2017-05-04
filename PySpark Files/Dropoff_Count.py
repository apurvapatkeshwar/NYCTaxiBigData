from numpy import array
from math import sqrt
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import split
from pyspark.sql.functions import col

#creating Spark Context
sc = SparkContext.getOrCreate()

#creating SQL Context
sqlContext = SQLContext(sc)

#loading the entire citibike data which contains individual monthwise data
data=sc.textFile("citibikedata")
header = data.first()

#the header names of November and december data differs, so taking into account their names as well
data2 = sc.textFile("citibikedata/201611-citibike-tripdata.csv")
header2 = data2.first()

#removing the header from the data
data = data.filter(lambda row : row != header and row != header2)
data = data.map(lambda row : row.replace('"',''))

#splitting the raw data which are "," separated
data = data.map(lambda x: x.split(",")).\
    map(lambda y: (float(y[9]),float(y[10])))

#defining the schema
customSchema = StructType([ \
    StructField("end_station_latitude", DoubleType(), True), \
    StructField("end_station_longitude", DoubleType(), True)])

#the start time is in MM/DD HH:MM format, extracting the date and time from this data
df = sqlContext.createDataFrame(data, customSchema)
split_col = split(df['starttime'], ' ')
df = df.withColumn('start_date', split_col.getItem(0))
df = df.withColumn('start_t', split_col.getItem(1))
df.show()

#grouping on the basis of endstation
groupby_station_count=df.groupby(['end_station_latitude','end_station_longitude']).count()
groupby_station_count.toPandas().to_csv('dropoff_count.csv')
