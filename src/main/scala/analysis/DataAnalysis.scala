package analysis
import preparation._
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression, LogisticRegressionModel}
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, VectorIndexer, OneHotEncoder, _}
import org.apache.spark.sql.types.{StringType}
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
    def trainLogisticRegression (data: DataFrame, spark: SparkSession): PipelineModel /*LogisticRegressionModel*/ = { // TODO

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
       
       val metrics = computeMetrics("prediction", "label", result, spark)


        println(s"\t\t\t${Console.BOLD}${Console.GREEN}Precision ${Console.RESET}")
        metrics.precisionByThreshold.foreach { case (t, p) =>
            println(s"Threshold: $t, Precision: $p")
        }

        println(s"\n\n\t\t\t${Console.BOLD}${Console.GREEN}Recall ${Console.RESET}")
        metrics.recallByThreshold.foreach { case (t, r) =>
            println(s"Threshold: $t, Recall: $r")
        }


        println(s"\n\n\t\t\t${Console.BOLD}${Console.GREEN}F-measure${Console.RESET}")
        metrics.fMeasureByThreshold(0.5).foreach { case (t, f) =>
            println(s"Threshold: $t, F-score: $f, Beta = 1")
        }
    
        println(s"\n\n\t\t\t${Console.BOLD}${Console.GREEN}ROC${Console.RESET}")
        print("area under ROC = ")
        println(metrics.areaUnderROC())

        model
    }

    /**
    * Carry out a prediction over a dataframe 
    * @param data: The dataframe the prediction will be carried out 
    * @param model: The pipeline trained model 
     */
    def predict (data: DataFrame, model: PipelineModel, spark: SparkSession): DataFrame = {
        val e = model.transform(data)
        .cache()
        .select("appOrSite", "bidfloor", "city", "exchange", "impid","interests",
        "media","network",  "os", "prediction",  "publisher", "size", 
        "timestamp",  "type", "user")
        DataFrameFunctions.prepareCSV(e)
         .withColumn("user", col("user").cast("String")).drop("prediction")
       
        
          // prepare for the csv creation
    }

    private def computeMetrics (prediction_colname: String, label_colname: String,
        testDF: DataFrame, spark: SparkSession): BinaryClassificationMetrics = {
        import spark.implicits._
        val predictionAndLabelRDD = testDF.select(prediction_colname, label_colname)
            .map(row => (row.getDouble(0), row.getDouble(1))).rdd

        new BinaryClassificationMetrics(predictionAndLabelRDD)
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
        //val normalizer = new Normalizer().setInputCol("features_temp").setOutputCol("features")
        val stdScaler = new StandardScaler().setInputCol("features_temp")
        .setWithStd(false).setWithMean(false)
        .setOutputCol("features")
        val l = lrModel:: stdScaler :: assembler :: colIndexerToListStringIndexers(colIndexers, 0)
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
            //.setTol(0.01)
            .setThreshold(0.3)
            //.setRegParam(0.01)
            .setMaxIter(10)
            //.setThreshold(0.3)
            .setRegParam(0.3)

        val pipelineStages = createPipeLineStages(arrayColumnIndexer, lr, assembler)

         new Pipeline().setStages(pipelineStages).fit(training_dataset)

        
    }


}
