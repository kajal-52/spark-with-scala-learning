package com.spark.exercise.apis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object TotalAmountSpentSorted {
  def extractCustomerPricePairs(line: String): (Int, Float)={
    val customerFields=line.split(",")
    (customerFields(0).toInt,customerFields(2).toFloat)
  }
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.INFO)

    val sc = new SparkContext("local[*]", "TotalAmountSpentByCustomer")

    val customerOrders = sc.textFile("src/main/resources/customer-orders.csv")
    val elementOfOrders = customerOrders.map(extractCustomerPricePairs)
    val amountSpentByCustomer = elementOfOrders.reduceByKey(( x, y ) => ( x + y ))

    //Sort results by amount spend by customer
    val amountSpentByCustomerPair = amountSpentByCustomer.map( x => (x._2, x._1))
    val amountSpentByCustomerSorted = amountSpentByCustomerPair.sortByKey()

    val totalAmountSpentByCustomer = amountSpentByCustomerSorted.collect()
    totalAmountSpentByCustomer.foreach(println)




  }

}
