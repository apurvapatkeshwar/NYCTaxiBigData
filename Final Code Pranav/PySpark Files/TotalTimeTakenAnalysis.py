from pyparsing import col
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import split
import pyspark.sql.functions


sc = SparkContext.getOrCreate()

data = sc.textFile("totaldistf/output")

# convert each line into a tuple
data = data.map(lambda x: x.split(",")).\
    map(lambda y: y[1])
sqlContext = SQLContext(sc)
customSchema = StructType([ \
    StructField("lpep_pickup_datetime", StringType(), True)])

df = sqlContext.createDataFrame(data, customSchema) #Creates a normal DataFrame
split_col = pyspark.sql.functions.split(df['lpep_pickup_datetime'], ' ')
df = df.withColumn('lpep_pickup_date', split_col.getItem(0))
df = df.withColumn('lpep_pickup_time', split_col.getItem(1))
split_col = pyspark.sql.functions.split(df['lpep_pickup_time'], ':')
df = df.withColumn('lpep_pickup_hour', split_col.getItem(0))
groupby_hour=df.groupby('lpep_pickup_hour').count().sort(col("lpep_pickup_hour").asc())
groupby_hour.toPandas().to_csv('hourly2.csv')




