package com.spark.exercise.objects

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StructType}

object TotalAmountSpentSortedDataset {
  case class Customer(custId:Int, itemId: Int, ItemPrice: Float)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)

    val sparkSession = SparkSession.builder()
      .appName("AmountSpentUsingDataset")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    val customerSchema = new StructType()
      .add("custId",IntegerType, nullable = true)
      .add("itemId", IntegerType, nullable = true)
      .add("itemPrice", FloatType, nullable = true)

    import sparkSession.implicits._
    val inputData = sparkSession.read
      .schema(customerSchema)
      .csv("src/main/resources/customer-orders.csv")
      .as[Customer]

    inputData.printSchema()
    val totalAmountSpent = inputData.select("custId","itemPrice").groupBy("custId").sum("itemPrice").withColumnRenamed("sum(itemPrice)","totalAmountSpent")
    totalAmountSpent.sort("totalAmountSpent").show()

  }

}
