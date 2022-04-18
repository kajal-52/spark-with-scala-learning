package com.spark.exercise.objects


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object MostObscureSuperHero {

  case class SuperHeroNames(movieId: Int, movieName: String)

  case class SuperHero(value: String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("MostObscureSuperHero")
      .master("local[*]")
      .config("spark.sql.warehouse.dir","/user/hive/warehouse")
      .getOrCreate()

    val superHeroSchema = new StructType()
      .add("movieId", IntegerType, nullable = true)
      .add("movieName", StringType, nullable = true)

    import spark.implicits._
    val inputNames = spark.read
      .schema(superHeroSchema)
      .option("sep"," ")
      .csv("src/main/resources/Marvel-names.txt")
      .as[SuperHeroNames]

    val lines = spark.read
      .text("src/main/resources/Marvel-graph.txt")
      .as[SuperHero]

    val connections = lines.withColumn("movieId",split(col("value"), " ")(0)).
      withColumn("connections", size(split(col("value"), " ") )- 1)
      .groupBy("movieId").agg(sum("connections").alias("connections"))

    // to compute min connection value
    val minConCount = connections.agg(min("connections")).first().getLong(0)

    val leastPopularSuperHeroIds = connections
      .filter($"connections"===minConCount)

    val leastPopularSuperHeroNames = leastPopularSuperHeroIds.join(inputNames, usingColumn = "movieId").select("movieName")
    leastPopularSuperHeroNames.show()


  }

}
