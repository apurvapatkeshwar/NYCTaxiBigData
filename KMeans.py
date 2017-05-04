from numpy import array
from math import sqrt
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark import SparkContext
from pyspark.sql import SQLContext
import math
from pyspark.sql.types import *
#from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors


def haversine(lat1, lng1, lat2, lng2):
    r = 6371
    dLat = Math.toRadians(lat2 - lat1);
    dLon = Math.toRadians(lng2 - lng1);
    a = math.sin(dLat / 2) * math.sin(dLat / 2) + math.cos(math.toRadians(lat1)) * math.cos(
        math.toRadians(lat2)) * math.sin(dLon / 2) * math.sin(dLon / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = r * c
    return d;


# Load and parse the data
#sc = SparkContext.getOrCreate()
sc = SparkContext()
data = sc.textFile("outputf3")
header = data.first()
data = data.filter(lambda row : row != header)
data = data.map(lambda x: x.split(",")).\
    map(lambda y: (int(y[0]), y[1], y[2],y[3],int(y[4]),float(y[5]),float(y[6]),float(y[7]),float(y[8]),int(y[9]),float(y[10]),float(y[11]),float(y[12]),float(y[13]),float(y[14]),float(y[15]),float(y[17]),float(y[18]),int(y[19]),int(y[20])))
sqlContext = SQLContext(sc)
customSchema = StructType([ \
    StructField("VendorID", IntegerType(), True), \
    StructField("lpep_pickup_datetime", StringType(), True), \
    StructField("Lpep_dropoff_datetime", StringType(), True), \
    StructField("Store_and_fwd_flag", StringType(), True), \
    StructField("RateCodeID", IntegerType(), True), \
    StructField("Pickup_longitude", DoubleType(), True), \
    StructField("Pickup_latitude", DoubleType(), True), \
    StructField("Dropoff_longitude", DoubleType(), True), \
    StructField("Dropoff_latitude", DoubleType(), True), \
    StructField("Passenger_count", IntegerType(), True), \
    StructField("Trip_distance", DoubleType(), True), \
    StructField("Fare_amount", DoubleType(), True), \
    StructField("Extra", DoubleType(), True), \
    StructField("MTA_tax", DoubleType(), True), \
    StructField("Tip_amount", DoubleType(), True), \
    StructField("Tolls_amount", DoubleType(), True), \
    StructField("improvement_surcharge", DoubleType(), True), \
    StructField("Total_amount", DoubleType(), True), \
    StructField("Payment_type", IntegerType(), True), \
    StructField("Trip_type", IntegerType(), True)])



cab_data = sc.textFile("yellow_tripdata_2016-01_cleaned.csv")
header = cab_data.first()
cab_data = cab_data.map(lambda x: x.split(",")).\
    map(lambda y: (float(y[6]),float(y[5])))

cab_customSchema = StructType([ \
     StructField("Pickup_latitude", DoubleType(), True), \
     StructField("Pickup_longitude", DoubleType(), True)])
cab_df = sqlContext.createDataFrame(cab_data, cab_customSchema)

df = sqlContext.createDataFrame(data, customSchema)
df2 = df[['Pickup_latitude', 'Pickup_longitude']]


df_concat = cab_df.unionAll(df2)

kmeans_rdd = df_concat.rdd.sortByKey()

modelInput = kmeans_rdd.map(lambda x: Vectors.dense(x[0],x[1])).sortByKey()

# Build the model (cluster the data)
clusters = KMeans.train(modelInput, 200, maxIterations=50, initializationMode="random")
centers = clusters.centers
print("Cluster Centers: ")
for center in centers:
    print(center)



data_citi=sc.textFile("citibikedata")
data_citi_diff_header=sc.textFile("citibikedata/201601-citibike-tripdata.csv")
header=data_citi.first()
header2=data_citi_diff_header.first()
data_citi = data_citi.filter(lambda row : row != header and row!=header2)
data_citi = data_citi.map(lambda row : row.replace('"',''))
data_citi = data_citi.map(lambda x: x.split(",")).\
    map(lambda y: (int(y[0]), y[1], y[2],int(y[3]),y[4],float(y[5]),float(y[6]),int(y[7]),y[8],float(y[9]),float(y[10]),int(y[11]),y[12],int(y[14])))
#

customSchema = StructType([ \
    StructField("tripduration", IntegerType(), True), \
    StructField("starttime", DateType(), True), \
    StructField("stoptime", DateType(), True), \
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

df_citi = sqlContext.createDataFrame(data_citi, customSchema)

location_df_cit=df_citi.select('start_station_latitude','start_station_longitude').distinct()
location_df_cit.show()

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))

# Save and load model
clusters.save(sc, "KModel")
#sameModel = KMeansModel.load(sc, "KModel")



