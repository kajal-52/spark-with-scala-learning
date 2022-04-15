package com.spark.exercise.objects

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, round}


object FriendByAge {

  case class Friend(id: Int, name: String, age: Int, friends: Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession
      .builder()
      .appName("FriendByAge")
      .master("local[*]")
      .getOrCreate()

    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import sparkSession.implicits._
    val friendDS = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/fakefriends.csv")
      .as[Friend]

    friendDS.printSchema()

    //Solving exercise using SQL query
    friendDS.createOrReplaceTempView("friends_by_age")
    val res=sparkSession.sql("SELECT age, ROUND(AVG(friends)) AS avg_no_of_friends FROM friends_by_age GROUP BY age ORDER BY age")
    res.show()

    //Solving exercise using dataset API functions
    val friendsByAge = friendDS.select(friendDS("age"),friendDS("friends"))
//    friendsByAge.show()

    friendsByAge.groupBy("age").avg("friends").show()

    // rounding of no of friends and sorting data by age
    friendsByAge.groupBy("age").agg(round(avg("friends"),2).as("avg_friends")).show()






  }

}
