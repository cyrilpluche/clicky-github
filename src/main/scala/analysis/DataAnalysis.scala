package analysis
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
        training.cache()
        val model = train(training)


        test.cache()
        val result =  model.transform(test)

        //println("End")
        result.printSchema()
        result.cache()
        //println(s" ${model.explainParams()}")
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
        val predictionAndLabelRDD = testDF.select(prediction_colname, label_colname)
            .map(row => (row.getDouble(0), row.getDouble(1))).rdd

        new BinaryClassificationMetrics(predictionAndLabelRDD, 10)
    }

    /**
    * Prepare data for a regression by changing String columon to StringIndex column then train
    * @param training_dataset: the dataFrame that will be prepared 
    * @return PipelineModel
    */
    private def train (training_dataset: DataFrame): PipelineModel = { 

        val appOrSiteIndexer = new StringIndexer().setInputCol("appOrSite")
            .setOutputCol("appOrSiteIndex")//.setHandleInvalid("keep")
        val cityIndexer = new StringIndexer().setInputCol("city")
            .setOutputCol("cityIndex").setHandleInvalid("keep")
        val exchangeIndexer = new StringIndexer().setInputCol("exchange")
            .setOutputCol("exchangeIndex").setHandleInvalid("keep")
        val impidIndexer = new StringIndexer().setInputCol("impid")
            .setOutputCol("impidIndex").setHandleInvalid("keep")
        val interestsIndexer = new StringIndexer().setInputCol("interests")
            .setOutputCol("interestsIndex").setHandleInvalid("keep")
        val mediaIndexer = new StringIndexer().setInputCol("media")
            .setOutputCol("mediaIndexer").setHandleInvalid("keep")
        val networkIndexer = new StringIndexer().setInputCol("network")
            .setOutputCol("networkIndex").setHandleInvalid("keep")
        val osIndexer = new StringIndexer().setInputCol("os")
            .setOutputCol("osIndex").setHandleInvalid("keep")
        val publisherIndexer = new StringIndexer().setInputCol("publisher")
            .setOutputCol("publisherIndex").setHandleInvalid("keep")
        val sizeIndexer = new StringIndexer().setInputCol("size")
            .setOutputCol("sizeIndex").setHandleInvalid("keep")
        //val typeIndexer = new StringIndexer().setInputCol("type")
            //.setOutputCol("typeIndex").setHandleInvalid("keep")
        val userIndexer = new StringIndexer().setInputCol("user")
            .setOutputCol("userIndex").setHandleInvalid("keep")

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
            .setMaxIter(15).setLabelCol("label").setFeaturesCol("features")

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


    def getArrayColumnIndexer(df: DataFrame): Array[ColumnIndexer] = {
        val columnNames = df.dtypes

        def colnamesToListColumnIndexer (list: List[ColumnIndexer], 
        colnames: Array[(String, String)], index: Int): List[ColumnIndexer] = {
            if (colnames.length == index) list
            else {
                val l = ColumnIndexer(colnames(index)._1, (colnames(index)._1).equals("StringType")) :: list
                colnamesToListColumnIndexer(l, colnames, index + 1)
            }
        }

        val listColunIndexer = colnamesToListColumnIndexer(Nil, columnNames, 0)
        listColunIndexer.toArray
    }
}
