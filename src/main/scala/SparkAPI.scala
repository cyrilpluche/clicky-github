import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkContext

/**
  * Created by toddmcgrath on 6/15/16.
  */
object SparkAPI extends App {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "ScalaProcess")
    val lines = sc.textFile("public/data-students.json")
    val output = lines.take(10)
    sc.stop()

    println(output(0))
    println(output(1))
    println(output(3))

    val spark = SparkSession.builder().getOrCreate()
    val data = spark.read.option("header", "true").option("inferSchema", "true").format("json")
          .load("public/data-studnets.json")
    data.printSchema()



}
