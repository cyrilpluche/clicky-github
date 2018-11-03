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

object DataAnalysis {

    //https://vishnuviswanath.com/spark_lr.html
    
    /**
    * perfoms a logisiticRegression
    * @param training_data: traning data set
    */
    def logisticRegression (training_data: DataFrame, spark: SparkSession): Any /*LogisticRegressionModel*/ = { // TODO

        val Array(training, test) = training_data.randomSplit(Array(0.8, 0.2), seed=12345)
        val model = train(training.cache())


        val df_test = test.cache()
        val result =  model.transform(df_test)

        //println("End")
        result.printSchema()
        println(s" ${model.explainParams()}")
        val nbErreur = result.filter(result.col("label") !== result.col("prediction")).count()
        println("Pourcentage erreur " + nbErreur/ result.count() + " Soit un nombre d'eerreur de " + nbErreur)
        result.filter(result.col("label") !== result.col("prediction")).select("prediction", "label").show(50, false)
       
       val metrics = testModel("label", "prediction", result, spark)
    
        print("ROC = ")
       println(metrics.areaUnderROC())
    }

    private def testModel (label_colname: String, prediction_colname: String, testDF: DataFrame, spark: SparkSession): 
    BinaryClassificationMetrics = {
        import spark.implicits._
        val predictionAndLabelRDD = testDF.select(label_colname, prediction_colname)
            .map(row => (row.getDouble(0), row.getDouble(1))).rdd

        new BinaryClassificationMetrics(predictionAndLabelRDD)
    }

    /**
    * Prepare data for a regression by changing String columon to StringIndex column then train
    * @param training_dataset: the dataFrame that will be prepared 
    * @return PipelineModel
    */
    private def train (training_dataset: DataFrame): PipelineModel = { 
        val appOrSiteIndexer = new StringIndexer().setInputCol("appOrSite")
            .setOutputCol("appOrSiteIndex").setHandleInvalid("skip")
        val cityIndexer = new StringIndexer().setInputCol("city")
            .setOutputCol("cityIndex").setHandleInvalid("skip")
        val exchangeIndexer = new StringIndexer().setInputCol("exchange")
            .setOutputCol("exchangeIndex").setHandleInvalid("skip")
        val impidIndexer = new StringIndexer().setInputCol("impid")
            .setOutputCol("impidIndex").setHandleInvalid("skip")
        val interestsIndexer = new StringIndexer().setInputCol("interests")
            .setOutputCol("interestsIndex").setHandleInvalid("skip")
        val mediaIndexer = new StringIndexer().setInputCol("media")
            .setOutputCol("mediaIndexer").setHandleInvalid("skip")
        val networkIndexer = new StringIndexer().setInputCol("network")
            .setOutputCol("networkIndex").setHandleInvalid("skip")
        val osIndexer = new StringIndexer().setInputCol("os")
            .setOutputCol("osIndex").setHandleInvalid("skip")
        val publisherIndexer = new StringIndexer().setInputCol("publisher")
            .setOutputCol("publisherIndex").setHandleInvalid("skip")
        val sizeIndexer = new StringIndexer().setInputCol("size")
            .setOutputCol("sizeIndex").setHandleInvalid("skip")
        //val typeIndexer = new StringIndexer().setInputCol("type")
            //.setOutputCol("typeIndex").setHandleInvalid("skip")
        val userIndexer = new StringIndexer().setInputCol("user")
            .setOutputCol("userIndex").setHandleInvalid("skip")

        /*var columns = data.columns // every column of dataFrame
                    .filterNot(_ == "label") // remove label column */
        
        val columns = Array(
            "appOrSiteIndex", "bidfloor", "cityIndex", "exchangeIndex", "impidIndex", 
            "interestsIndex", "label", "mediaIndexer", "networkIndex", "osIndex", 
            "publisherIndex", "sizeIndex", "timestamp","type", "userIndex")
        val assembler = new VectorAssembler()
            .setInputCols(columns).setOutputCol("features_temp")

        val normalizer = new Normalizer().setInputCol("features_temp").setOutputCol("features")

        val lr = new LogisticRegression().setFitIntercept(true)
            .setStandardization(true).setRegParam(0.3).setTol(0.1)
            .setMaxIter(10).setLabelCol("label").setFeaturesCol("features")

        val pipeline = new Pipeline().setStages(
            Array(appOrSiteIndexer, cityIndexer, exchangeIndexer, impidIndexer, interestsIndexer, 
            mediaIndexer, networkIndexer, osIndexer, publisherIndexer, sizeIndexer, //typeIndexer, 
            userIndexer, assembler, normalizer, lr)
        )

       

        println(s"\t\t\t${Console.RED}${Console.BOLD}Start data analysis${Console.RESET}")
        print(s"-\ttraining: ")
        
        val model = pipeline.fit(training_dataset)
        print(s"\t\t${Console.BLUE}${Console.BOLD}Maching learning finished ${Console.RESET}")

        model
        
        //println(s" ${model.explainParams()}")
        
        
    }
}
