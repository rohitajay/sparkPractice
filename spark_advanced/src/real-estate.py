from __future__ import print_function

from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

if __name__ == "__main__":

    # Create a spark session
    spark = SparkSession.builder.appName("DecisionTree").getOrCreate()

    #Load up data as dataframe
    data = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/administrator/PycharmProjects/sparkProject/spark_advanced/Data/realestate.csv")

    assembler = VectorAssembler().setInputCols(["HouseAge", "DistanceToMRT", "NumberConvenienceStores"]).setOutputCol("features")

    df = assembler.transform(data).select("PriceOfUnitArea", "features")

    # Lets split our data into training data and test data
    trainTest = df.randomSplit([0.5, 0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    # Now create our decision tree
    dtr = DecisionTreeRegressor().setFeaturesCol("features").setLabelCol("PriceOfUnitArea")

    # Train the model using our training data
    model = dtr.fit(trainingDF)

    fullPredictions = model.transform(testDF).cache()

    #Extract the predictions and the known correct labels
    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("PriceOfUnitArea").rdd.map(lambda x: x[0])
    # print(predictions.collect(),labels.collect())

    # Zip them together
    predictionAndLabel = predictions.zip(labels).collect()
    # print(predictionAndLabel)
    #Print out the predicted and actual values for each point
    for prediction in predictionAndLabel:
        print(prediction)
        # pass

    spark.stop()