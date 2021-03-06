name := "clicky-github"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.2"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies += "com.twitter" %% "util-eval" % "[6.2.4,)"
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.0"
libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
libraryDependencies += "org.fusesource.jansi" % "jansi" % "1.17.1"
lazy val scalacheck = "org.scalacheck" %% "scalacheck" % "1.13.4"
libraryDependencies += scalacheck % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "mysql" % "mysql-connector-java" % "5.1.6"
)
