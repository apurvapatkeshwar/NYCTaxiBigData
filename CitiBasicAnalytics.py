from numpy import array
from pyspark.mllib.clustering import KMeans, KMeansModel
from math import sqrt
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import split
from pyspark.sql.functions import col

#Creating SparkContext
sc = SparkContext.getOrCreate()
#Creating SQLContext for the RDD to pyspark dataframe for easy analytics
sqlContext = SQLContext(sc)

#Loading the complete cleaned Citibike Data present in HDFS
data=sc.textFile("citibikedata")
#Getting the header of the citibike data (csv file)
header = data.first()

#The header of November and December differs, so loading that data and taking its header
data2 = sc.textFile("citibikedata/201611-citibike-tripdata.csv")
header2 = data2.first()

#Removing all rows which are headers
data = data.filter(lambda row : row != header and row != header2)

#Removing the " character present in the raw input lines
data = data.map(lambda row : row.replace('"',''))

#Splitting the raw input lines and giving them the datatype to make a RDD
data = data.map(lambda x: x.split(",")).\
    map(lambda y: (int(y[0]), y[1], y[2],int(y[3]),y[4],float(y[5]),float(y[6]),int(y[7]),y[8],float(y[9]),float(y[10]),int(y[11]),y[12],int(y[14])))

#Converting the raw data into the datatype of RDD
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

#Converting the RDD to pyspark dataframe
df = sqlContext.createDataFrame(data, customSchema)

#The starttime is a string which contains information in the form of MM/DD HH:MM, so extracting them and creating new columns
split_col = split(df['starttime'], ' ')
df.show()
df = df.withColumn('start_date', split_col.getItem(0))
df = df.withColumn('start_t', split_col.getItem(1))
df.show()

#Splitting the date in MM/DD format created to get month and date
split_col1 = split(df['start_date'], "/")
df = df.withColumn('start_month', split_col1.getItem(0))
df = df.withColumn('start_d', split_col1.getItem(1))
df.show()

#Splitting the time in HH:MM format created to get hour and minute
split_col2 = split(df['start_t'], ':')
df = df.withColumn('start_hour', split_col2.getItem(0))
df = df.withColumn('start_minute', split_col2.getItem(1))

#Getting all station ids of Citibike
distinctValues = df.select('start_station_latitude','start_station_longitude').distinct()
distinctValues.toPandas().to_csv('stations.csv')

#Grouping the data according to gender
groupby_gender=df.groupby('gender').count()
groupby_gender.toPandas().to_csv('gender_analysis.csv')

#Calculating the pickup frequency of each stations
groupby_station_count=df.groupby(['start_station_latitude','start_station_longitude']).count()
groupby_station_count.toPandas().to_csv('usage_count.csv')


