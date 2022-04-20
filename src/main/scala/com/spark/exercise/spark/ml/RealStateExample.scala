package com.spark.exercise.spark.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql.SparkSession

object RealStateExample {
  case class RealStateSchema(No: Int,TransactionDate: Double,HouseAge: Double,DistanceToMRT: Double,NumberConvenienceStores: Int,Latitude: Double,Longitude: Double,PriceOfUnitArea: Double)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("RealStatePricePredictor")
      .getOrCreate()

    import spark.implicits._
    val realstatedata = spark.read.
      option("header", "true")
      .option("inferSchema","true")
      .csv("src/main/resources/realestate.csv")
      .as[RealStateSchema]

    val assembler = new VectorAssembler()
      .setInputCols(Array("HouseAge","DistanceToMRT","NumberConvenienceStores"))
      .setOutputCol("features")

    val df = assembler.transform(realstatedata)
      .select("features", "PriceOfUnitArea")

    val trainValidationSplit = df.randomSplit(Array(0.7,0.3))
    val trainingDf = trainValidationSplit(0)
    val predictDf = trainValidationSplit(1)
    val dtr = new DecisionTreeRegressor()
      .setLabelCol("PriceOfUnitArea")
      .setFeaturesCol("features")

    val model = dtr.fit(trainingDf)

    val prediction = model.transform(predictDf).cache()
    val predictionAndLabel = prediction.select("prediction","PriceOfUnitArea").collect()

    for (predictedData <- predictionAndLabel){
      println(predictedData)
    }
  }

}
