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
  * By order of the Clicky-Blinders
  */
object SparkAPI extends App {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().config("spark.master","local").getOrCreate()
    spark.conf.set("spark.sql.codegen.wholeStage", false)

    import spark.implicits._

    //val data = spark.read.json("public/data-students.json")
    val data = spark.read.format("json").load("public/data-students.json")
    data.printSchema()
    
    var training_data_cleaned = DataCleaner.clean(data, spark)
    training_data_cleaned = training_data_cleaned.cache()
     print("\033[H\033[2J") // delete everything on the screen
    println(s"\t\t\t${Console.YELLOW}${Console.BOLD}Cleanning data is finished ${Console.RESET}")
    training_data_cleaned.printSchema()


    

    //training_data_cleaned.write.format("json").save("public/datacleaned.json")
    DataAnalysis.logisticRegression(training_data_cleaned, spark)

    println(s"\n\n\n\t\t${Console.BOLD}${Console.UNDERLINED}${Console.RED}By order of the Clicky Blinder${Console.RESET}")
    spark.stop()


}
