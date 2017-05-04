from numpy import array
from math import sqrt
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import split
from pyspark.sql.functions import col

#creating the spark context
sc = SparkContext.getOrCreate()

#creating the SQLContext
sqlContext = SQLContext(sc)

#the header of November and December month differs from the rest of the month, sol loading Decembers data and getting
data2 = sc.textFile("citibikedata/201612-citibike-tripdata.csv")
header2 = data2.first()

#reading the complete citibike data
data=sc.textFile("citibikedata")
#taking its header
header = data.first()

#removing the header rows from the data
data = data.filter(lambda row : row != header and row != header2)
data = data.map(lambda row : row.replace('"',''))

#splitting the raw "," separated data into appropriate format
data = data.map(lambda x: x.split(",")).\
    map(lambda y: (y[1],float(y[5]),float(y[6])))

#creating the schema for dataframe
customSchema = StructType([ \
    StructField("starttime", StringType(), True), \
    StructField("start_station_latitude", DoubleType(), True), \
    StructField("start_station_longitude", DoubleType(), True)])

#creating the dataframe
df = sqlContext.createDataFrame(data, customSchema)
split_col = split(df['starttime'], ' ')
df.show()


#the start time is in MM/DD HH:MM format, extracting the date and time from this data
df = sqlContext.createDataFrame(data, customSchema)
split_col = split(df['starttime'], ' ')
df = df.withColumn('start_date', split_col.getItem(0))
df = df.withColumn('start_t', split_col.getItem(1))
df.show()

#the date is in format MM/DD, extracting the date and month from this
split_col1 = split(df['start_date'], "/")
df = df.withColumn('start_month', split_col1.getItem(0))
df = df.withColumn('start_d', split_col1.getItem(1))
df.show()

#the time is in HH:MM format, extracting the hour and minute information from this
split_col2 = split(df['start_t'], ':')
df = df.withColumn('start_hour', split_col2.getItem(0))
df = df.withColumn('start_minute', split_col2.getItem(1))

groupby_station_count=df.groupby(['start_station_latitude','start_station_longitude']).count()
groupby_station_count.toPandas().to_csv('pickup_count.csv')
