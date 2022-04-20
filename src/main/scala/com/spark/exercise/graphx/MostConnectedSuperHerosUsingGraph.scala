package com.spark.exercise.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}


object MostConnectedSuperHerosUsingGraph {

  //function to extract heroId and heroName tuple
  def parseNames(line: String) : Option[(VertexId, String)] = {
    val fields = line.split('\"')
    if (fields.length > 1) {
      val heroID:Long = fields(0).trim().toLong
      if (heroID < 6487) {  // ID's above 6486 aren't real characters
        return Some( fields(0).trim().toLong, fields(1))
      }
    }

    None // flatmap will just discard None results, and extract data from Some results.
  }
  def makeEdges(line :String): List[Edge[Int]]={
    import scala.collection.mutable.ListBuffer
    var edges = new ListBuffer[Edge[Int]]()
    val fields= line.split(" ")
    val origin = fields(0)
    for (x <- 1 until fields.length - 1){
      edges += Edge(origin.toLong, fields(x).toLong,0)
    }
    return edges.toList
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)

    val sc = new SparkContext("local[*]", "SparkContext")
    val names = sc.textFile("src/main/resources/Marvel-names.txt")

    val vertices = names.flatMap(parseNames)

    val lines = sc.textFile("src/main/resources/Marvel-graph.txt")
    val edges = lines.flatMap(makeEdges)

    val default = "Nobody"
    val graph = Graph(vertices, edges, default).cache()
    println("\nTop 10 most connected superheroes:")

    // The join merges the hero names into the output; sorts by total connections on each node.
    graph.degrees.join(vertices).sortBy(_._2._1, ascending = false).take(10).foreach(println)


  }

}
