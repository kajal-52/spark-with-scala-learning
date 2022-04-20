ThisBuild / name := "spark-with-scala-learning"

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "org.apache.spark" %% "spark-mllib" % "2.4.5",
  "org.apache.spark" %% "spark-streaming" % "2.4.5",
//  "org.twitter4j" % "twitter4j-core" % "4.0.4",
//  "org.twitter4j" % "twitter4j-stream" % "4.0.4"
)
