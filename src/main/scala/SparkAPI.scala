import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import collection.mutable._
import scala.collection.JavaConversions._

/**
  * Created by toddmcgrath on 6/15/16.
  */
object SparkAPI extends App {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().config("spark.master","local").getOrCreate()

    import spark.implicits._

    val data = spark.read.json("public/data-students.json")
    data.printSchema()
    

    val d2 = Cleaner.clean(data, spark)
    d2.select("label").show()
    d2.select("size").show()
    d2.select("os").show()
    d2.select("bidfloor").show()
    d2.select("type").show()
  
    //val data1=data.select("label").map(x=> if (x(0)==(true)) 1 else 0)

    /*val v = data.select("size").map(r => {
      println(r.toSeq.toList.head(0))
      "Salut"
    } )
    //println(data1.isInstanceOf[DataFrame])
    v.show()*/
    spark.stop()


    /*
        Enzo: size, label, os, bidfloor
     */

}
