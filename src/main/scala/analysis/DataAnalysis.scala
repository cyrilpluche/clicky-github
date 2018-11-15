package analysis
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

object DataAnalysis {

    //https://vishnuviswanath.com/spark_lr.html
    
    /**
    * perfoms a logisiticRegression
    * @param data: traning data set
    */
    def logisticRegression (data: DataFrame, spark: SparkSession): Any /*LogisticRegressionModel*/ = { // TODO

        val Array(training, test) =data.randomSplit(Array(0.8, 0.2), seed=1L)
        /* DataFrameFunctions.randomSplit(data, Array(0.8, 0.2)) */ 
        val df_train = training.cache()
        //val model = train(df_train)
         println(s"\t\t\t${Console.RED}${Console.BOLD}Start data analysis${Console.RESET}")
        val model = createPipeLineModel(df_train)
        val lrm:LogisticRegressionModel=model.stages.last.asInstanceOf[LogisticRegressionModel]
        println(lrm.coefficients)
        println(s"\n\n\t\t${Console.BLUE}${Console.BOLD}Machine learning finished ${Console.RESET}")
        val df_test = test.cache()
        val result =  model.transform(df_test)

        result.printSchema()
        result.cache()
       
       val metrics = testModel("label", "prediction", result, spark)


        println(s"\t\t\t${Console.BOLD}${Console.GREEN}Precision ${Console.RESET}")
        metrics.precisionByThreshold.foreach { case (t, p) =>
            println(s"Threshold: $t, Precision: $p")
        }

        println(s"\n\n\t\t\t${Console.BOLD}${Console.GREEN}Recall ${Console.RESET}")
        metrics.recallByThreshold.foreach { case (t, r) =>
            println(s"Threshold: $t, Recall: $r")
        }


        println(s"\n\n\t\t\t${Console.BOLD}${Console.GREEN}F-measure${Console.RESET}")
        metrics.fMeasureByThreshold.foreach { case (t, f) =>
            println(s"Threshold: $t, F-score: $f, Beta = 1")
        }
    
        println(s"\n\n\t\t\t${Console.BOLD}${Console.GREEN}ROC${Console.RESET}")
        print("area under ROC = ")
        println(metrics.areaUnderROC())
    }

    private def testModel (label_colname: String, prediction_colname: String, 
        testDF: DataFrame, spark: SparkSession): BinaryClassificationMetrics = {
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
    private def train (training_dataset: DataFrame): PipelineModel = { // not used anymore

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
            "appOrSiteIndex", "bidfloor", "exchangeIndex", 
            "interestsIndex", "mediaIndexer", "networkIndex", "osIndex", 
            "publisherIndex", "sizeIndex", "timestamp","type", "userIndex")
        val assembler = new VectorAssembler()
            .setInputCols(columns).setOutputCol("features_temp")

        val normalizer = new Normalizer().setInputCol("features_temp").setOutputCol("features")

        val lr = new LogisticRegression().setFitIntercept(true)
            .setStandardization(true).setRegParam(0.1).setTol(0.1)
            .setMaxIter(10).setLabelCol("label").setFeaturesCol("features")

        val pipeline = new Pipeline().setStages(
            Array(appOrSiteIndexer,  exchangeIndexer,  interestsIndexer, 
            mediaIndexer, networkIndexer, osIndexer, publisherIndexer, sizeIndexer, //typeIndexer, 
            userIndexer, assembler, normalizer, lr)
        )

       

        println(s"\t\t\t${Console.RED}${Console.BOLD}Start data analysis${Console.RESET}")
        print(s"-\ttraining: ")
        
        val model = pipeline.fit(training_dataset)
        print(s"\t\t${Console.BLUE}${Console.BOLD}Machine learning finished ${Console.RESET}")

        model
        
        //println(s" ${model.explainParams()}")
        
        
    }


    def getArrayColumnIndexer(df: DataFrame): Array[ColumnIndexer] = {
        val columnNames = df.dtypes.filter(!_._1.equals("label"))

        /**
        * @param colnames: Array of tuples (colname, coltype)
        * @param index: The index of the current case of the array
        */
        def colnamesToListColumnIndexer (colnames: Array[(String, String)], index: Int): List[ColumnIndexer] = {
            if (colnames.length == index) Nil
            else {

                // Check if the type of the column is String otherwise no need of StringIndexer
                ColumnIndexer(colnames(index)._1, (colnames(index)._2).equals("StringType")) :: 
                    colnamesToListColumnIndexer(colnames, index + 1)
            }
        }

        val listColunIndexer = colnamesToListColumnIndexer(columnNames, 0)
        listColunIndexer.toArray
    }

    def createVecteurAssembler (colIndexers: Array[ColumnIndexer]): VectorAssembler = {

        def colIndexersToListNames (arrayIndexers: Array[ColumnIndexer], index: Int):
         List[String] = {
             if (index == arrayIndexers.length) Nil
             else 
                 arrayIndexers(index).getOutPutName :: colIndexersToListNames(arrayIndexers, index + 1)
        }

        val colums = colIndexersToListNames(colIndexers, 0).toArray
        new VectorAssembler().setInputCols(colums).setOutputCol("features_temp")
    }

    /**
    * @param colIndexers: Array of ColumnIndexer
    * @param lrModel: Logisitic Regression
    * @param vectorAssembler: vector assembler
    * @return Array[PipelineStage]
    */
    def createPipeLineStages (colIndexers: Array[ColumnIndexer], lrModel: LogisticRegression,
         assembler: VectorAssembler): 
        Array[PipelineStage] = {

        def colIndexerToListStringIndexers (array: Array[ColumnIndexer], index: Int): List[PipelineStage]= {
            if (array.length == index) Nil
            else {
                if (array(index).createIndex.isEmpty) colIndexerToListStringIndexers(array, index + 1)
                else array(index).createIndex.get :: colIndexerToListStringIndexers(array, index + 1)
            }
        }
        val normalizer = new Normalizer().setInputCol("features_temp").setOutputCol("features")
        val l = lrModel:: normalizer ::assembler :: colIndexerToListStringIndexers(colIndexers, 0)
        l.reverse.toArray // reverse the list because assembler cannot before the StringIndexer
    }

    /**
    * @param training_dataset: DataFrame the model will train on
    * Starting from a dataframe train and create a PipeLineModel 
    * the DataFrame is supposed to be provided for training so it was split before
    */
    def createPipeLineModel (training_dataset: DataFrame): PipelineModel = {
        val arrayColumnIndexer = getArrayColumnIndexer(training_dataset)
        val assembler = createVecteurAssembler(arrayColumnIndexer)

        val lr = new LogisticRegression()//.setFitIntercept(true)
            //.setStandardization(true).setRegParam(0.1)
            //.setElasticNetParam(0.6)//
            .setTol(0.1)
            //.setRegParam(0.01)
            .setMaxIter(10)
            //.setThreshold(0.3)
            //.setRegParam(0.2)

        val pipelineStages = createPipeLineStages(arrayColumnIndexer, lr, assembler)

         new Pipeline().setStages(pipelineStages).fit(training_dataset)

        
    }


}
