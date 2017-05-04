from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession

from pyspark import SparkContext, SparkConf
from pyspark.mllib.util import MLUtils

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("RandomForestRegressorExample") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    #Reads the data in the LIBSVM format
    #The format of the libsvm was followed
    data = spark.read.format("libsvm").load("modsmall.csv") #Loads the data

    #Set up the configurations
    conf = SparkConf().setAppName('Prediction of Time')
    sc = SparkContext(conf=conf)

    data = MLUtils.loadLibSVMFile(sc, 'modsmall.csv')

    # Split the data into training and test sets (20% held out for testing)
    (trainingData, testData) = data.randomSplit([0.8, 0.2])

    rf = RandomForestRegressor(featuresCol="indexedFeatures")

    pipeline = Pipeline(stages=[rf])

    model = pipeline.fit(trainingData)


    prediction = model.transform(testData)

    prediction.select("prediction", "label", "features").show(2)

    evaluator = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(prediction)
    print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

    rfModel = model.stages[1]
    print(rfModel)  # summary only




