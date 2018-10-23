import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

/**
  * Created by toddmcgrath on 6/15/16.
  */
object SparkAPI extends App {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().config("spark.master","local").getOrCreate()
    val data = spark.read.option("header", "true").option("inferSchema", "true").format("json")
          .load("public/data-students.json")
    data.printSchema()
    spark.stop()


}
