from numpy import array
from math import sqrt
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark import SparkContext
from pyspark.sql import SQLContext
import math
from pyspark.sql.types import *
#from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors

#formula used to compute distance between 2 location
def haversine(lat1, lng1, lat2, lng2):
    r = 6371
    dLat = Math.toRadians(lat2 - lat1);
    dLon = Math.toRadians(lng2 - lng1);
    a = math.sin(dLat / 2) * math.sin(dLat / 2) + math.cos(math.toRadians(lat1)) * math.cos(
        math.toRadians(lat2)) * math.sin(dLon / 2) * math.sin(dLon / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = r * c
    return d;



sc = SparkContext()
sqlContext = SQLContext(sc)
#reading green taxis data
data = sc.textFile("outputf3")
header = data.first()
data = data.filter(lambda row : row != header)
#removing the header, splitting the raw ',' separated string and giving them the datatypes to create RDD
data = data.map(lambda x: x.split(",")).\
    map(lambda y: (int(y[0]), y[1], y[2],y[3],int(y[4]),float(y[5]),float(y[6]),float(y[7]),float(y[8]),int(y[9]),float(y[10]),float(y[11]),float(y[12]),float(y[13]),float(y[14]),float(y[15]),float(y[17]),float(y[18]),int(y[19]),int(y[20])))

#defining the schema of green taxis
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

#converting green cabs RDD to pyspark dataframe
df = sqlContext.createDataFrame(data, customSchema)
#subsetting to get the required columns
df2 = df[['Pickup_latitude', 'Pickup_longitude']]

#reading yellow taxis data
cab_data = sc.textFile("yellow_tripdata_2016-01_cleaned.csv")
header = cab_data.first()
#removing the header, splitting the raw ',' separated string and giving them the datatypes to create RDD
cab_data = cab_data.filter(lambda row : row != header)
cab_data = cab_data.map(lambda x: x.split(",")).\
    map(lambda y: (float(y[6]),float(y[5])))

#subsetting the yellow taxi data along columns
cab_customSchema = StructType([ \
     StructField("Pickup_latitude", DoubleType(), True), \
     StructField("Pickup_longitude", DoubleType(), True)])

#converting yellowcabs RDD to pyspark dataframe
cab_df = sqlContext.createDataFrame(cab_data, cab_customSchema)


#merging green and yellow taxi data frames
df_concat = cab_df.unionAll(df2)
kmeans_rdd = df_concat.rdd.sortByKey()

modelInput = kmeans_rdd.map(lambda x: Vectors.dense(x[0],x[1])).sortByKey()

# Building the model (cluster the data)
clusters = KMeans.train(modelInput, 200, maxIterations=50, initializationMode="random")
centers = clusters.centers
print("Cluster Centers: ")
for center in centers:
    print(center)



# Evaluating the clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))

# Saving the  model
clusters.save(sc, "KModel")

#To use this model, commans is as follows
#sameModel = KMeansModel.load(sc, "KModel")



