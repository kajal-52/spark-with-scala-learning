package com.spark.exercise.apis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object SquareItRdd {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //As Scala is functional programming language,  many RDD methods accept function as a parameter
    val sc = new SparkContext("local[*]", "SparkContext")
    val rdd = sc.parallelize(List(1, 2, 3, 4))
    val res1 = rdd.map(x => x * x)
    res1.foreach(println)

    // Creating a squareIt function and then passing to map

    def squareIt(x: Int): Int = {
      return x * x
    }

    val res = rdd.map(squareIt)
    res.foreach(println)
  }

}
