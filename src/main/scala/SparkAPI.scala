import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

/**
  * Created by toddmcgrath on 6/15/16.
  */
object SparkAPI extends App {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().config("spark.master","local").getOrCreate()
    import spark.implicits._
    val data = spark.read.json("public/data-students.json")
    data.printSchema()
    data.select("label").show()
    val data1=data.select("label").map(x=>(if(x(0)==true)1 else 0))
    data1.show()
    spark.stop()


    /*
        Enzo: size, label, os, bidfloor
     */

}
