import analysis._
import preparation._
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression, LogisticRegressionModel}
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, VectorIndexer, OneHotEncoder, _}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml._
import scala.annotation.tailrec
import org.apache.spark.ml.util._

/**
  * By order of the Clicky-Blinders
  */
object SparkAPI extends App {
   Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().config("spark.master","local").getOrCreate()
    spark.conf.set("spark.sql.codegen.wholeStage", false)
    import spark.implicits._

    print("\033[H\033[2J") // delete everything on the screen
    println("Welcome to the click prediction algorithm.\nWhat do you want to do ?")
    println("1-\t Train a model (machine learning that will result on a model)")
    println("2-\t Predict on my dataset (provide your own dataset)\n")
    println("Please chose between these two options")
    val choice = scala.io.StdIn.readInt


   

    choice match {

      case 1 => 

        println(s"${Console.BOLD}Train a model${Console.RESET}\n")
          //val data = spark.read.json("public/data-students.json")
        val data = spark.read.format("json").load("public/data-students.json")
        data.printSchema()
        
        var training_data_cleaned = DataCleaner.clean(data, spark)
        training_data_cleaned = training_data_cleaned.cache()
        print("\033[H\033[2J") // delete everything on the screen
        println(s"\t\t\t${Console.YELLOW}${Console.BOLD}Cleanning data is finished ${Console.RESET}")
        training_data_cleaned.printSchema
        // city, interest, impid, timestamp, user

        //training_data_cleaned.write.format("json").save("public/datacleaned.json")
        val pModel = DataAnalysis.trainLogisticRegression(training_data_cleaned, spark)
         pModel.write.overwrite().save("public/model_trained")

      case 2 => 

      println(s"${Console.BOLD}Predict on my dataset${Console.RESET}\n- First load your dataset")
      println("- Then the model will predict")
      try {
          val model = PipelineModel.read.load("public/model_trained") // load the model
          val filename = scala.io.StdIn.readLine("Path to your file in json format\n")
          val data = spark.read.format("json").load("public/data-students.json")
      } catch {
        case _ : Throwable => println(s"${Console.BOLD}${Console.RED}Error unable to load these file maybe try to train the model before${Console.RESET}")
      }
    

      case _ => println(s"${Console.BOLD}${Console.RED}Wrong option please restart the program and choose between option 1 or 2${Console.RESET}\n")
    }

    println(s"\n\n\n\t\t${Console.BOLD}${Console.UNDERLINED}${Console.RED}By order of the Clicky Blinder${Console.RESET}")
    spark.stop()


}
