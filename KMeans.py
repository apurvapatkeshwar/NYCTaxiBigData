from numpy import array
from math import sqrt
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
#from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors
# Load and parse the data
sc = SparkContext.getOrCreate()
#sc = SparkContext()
data = sc.textFile("smalltaxidata.csv")
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

df = sqlContext.createDataFrame(data, customSchema)
df2 = df[['Pickup_latitude', 'Pickup_longitude']]
kmeans_rdd = df2.rdd.sortByKey()
modelInput = kmeans_rdd.map(lambda x: Vectors.dense(x[0],x[1])).sortByKey()

# Build the model (cluster the data)
clusters = KMeans.train(modelInput, 10, maxIterations=50, initializationMode="random")

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))

# Save and load model
clusters.save(sc, "KModel")
sameModel = KMeansModel.load(sc, "KModel")



#VendorID	lpep_pickup_datetime	Lpep_dropoff_datetime	Store_and_fwd_flag	RateCodeID	Pickup_longitude
# Pickup_latitude	Dropoff_longitude	Dropoff_latitude	Passenger_count	Trip_distance	Fare_amount	Extra
# MTA_tax	Tip_amount	Tolls_amount	Ehail_fee	improvement_surcharge	Total_amount	Payment_type	Trip_type




