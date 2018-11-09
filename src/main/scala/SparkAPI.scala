import analysis._
import preparation._
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import collection.mutable._
import scala.collection.JavaConversions._
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression, LogisticRegressionModel, _}
import org.apache.spark.mllib.util.MLUtils
/**
  * Created by Clicky-Blinders.
  */
object SparkAPI extends App {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().config("spark.master","local").getOrCreate()
    spark.conf.set("spark.sql.codegen.wholeStage", false)

    import spark.implicits._

    //val data = spark.read.json("public/data-students.json")
    val data = spark.read.format("json").load("public/data-students.json")
    data.printSchema()
    /*data.filter(data.col("appOrSite").isNull).select("appOrSite").show(1)
    data.filter(data.col("bidfloor").isNull).select("bidfloor").show(1)
    data.filter(data.col("city").isNull).select("city").show(1)
    data.filter(data.col("exchange").isNull).select("exchange").show(1)
    data.filter(data.col("impid").isNull).select("impid").show(1)
    data.filter(data.col("interests").isNull).select("interests").show(1)
    data.filter(data.col("media").isNull).select("media").show(1)
    data.filter(data.col("network").isNull).select("network").show(1)
    data.filter(data.col("os").isNull).select("os").show(1)
    data.filter(data.col("publisher").isNull).select("publisher").show(1)
    data.filter(data.col("size").isNull).select("size").show(1)
    data.filter(data.col("timestamp").isNull).select("timestamp").show(1)
    data.filter(data.col("type").isNull).select("type").show(1)
     data.filter(data.col("user").isNull).select("user").show(1)*/

    data.select("impid", "timestamp").show(70)
    var training_data_cleaned = DataCleaner.clean(data, spark)
    training_data_cleaned = training_data_cleaned.cache()
     print("\033[H\033[2J") // delete everything on the screen
    println(s"\t\t\t${Console.YELLOW}${Console.BOLD}Cleanning data is finished ${Console.RESET}")
    training_data_cleaned.printSchema()

    /*training_data_cleaned.filter(training_data_cleaned.col("appOrSite").isNull).select("appOrSite").show(1)
    training_data_cleaned.filter(training_data_cleaned.col("bidfloor").isNull).select("bidfloor").show(1)
    training_data_cleaned.filter(training_data_cleaned.col("city").isNull).select("city").show(1)
    training_data_cleaned.filter(training_data_cleaned.col("exchange").isNull).select("exchange").show(1)
    training_data_cleaned.filter(training_data_cleaned.col("impid").isNull).select("impid").show(1)
    training_data_cleaned.filter(training_data_cleaned.col("interests").isNull).select("interests").show(1)
    training_data_cleaned.filter(training_data_cleaned.col("media").isNull).select("media").show(1)
    training_data_cleaned.filter(training_data_cleaned.col("network").isNull).select("network").show(1)
    training_data_cleaned.filter(training_data_cleaned.col("os").isNull).select("os").show(1)
    training_data_cleaned.filter(training_data_cleaned.col("publisher").isNull).select("publisher").show(1)
    training_data_cleaned.filter(training_data_cleaned.col("size").isNull).select("size").show(1)
    training_data_cleaned.filter(training_data_cleaned.col("timestamp").isNull).select("timestamp").show(1)
    training_data_cleaned.filter(training_data_cleaned.col("type").isNull).select("type").show(1)
     training_data_cleaned.filter(training_data_cleaned.col("user").isNull).select("user").show(1) */
    
    // training_data_cleaned.select("network").show()
    // training_data_cleaned.select("label").show()
    // training_data_cleaned.select("size").show()
    // training_data_cleaned.select("os").show()
    // training_data_cleaned.select("bidfloor").show()
    // training_data_cleaned.select("type").show()
    

    //training_data_cleaned.select("impid", "timestamp", "user", "publisher", "exchange").show(70)
    //training_data_cleaned.select("label").show()
    //training_data_cleaned.select("size").show()
    //training_data_cleaned.select("os").show()
    //training_data_cleaned.select("bidfloor").show()
    //training_data_cleaned.select("type").show()
    //training_data_cleaned.select("interests").show(100, false)
  
    //val data1=data.select("label").map(x=> if (x(0)==(true)) 1 else 0)

    /*val v = data.select("size").map(r => {
      println(r.toSeq.toList.head(0))
      "Salut"
    } )
    //println(data1.isInstanceOf[DataFrame])
    v.show()*/

    /*val splitted = training_data_cleaned.randomSplit(Array(0.8, 0.2))
    val lrModel = DataAnalysis.logisticRegression(splitted(0))
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")*/

    //training_data_cleaned.write.format("json").save("public/datacleaned.json")
    DataAnalysis.logisticRegression(training_data_cleaned, spark)
    spark.stop()


    /*
        Enzo: size, label, os, bidfloor
     */

}
