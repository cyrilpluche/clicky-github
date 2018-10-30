import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression, LogisticRegressionModel}
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, VectorIndexer, OneHotEncoder}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.Pipeline
import scala.annotation.tailrec

object DataAnalysis {

    //https://vishnuviswanath.com/spark_lr.html
    
    /**
    * perfoms a logisiticRegression
    * @param training_data: traning data set
    */
    def logisticRegression (training_data: DataFrame): Any /*LogisticRegressionModel*/ = { // TODO
        prepareData(training_data)
        /*var columns = training_data.columns
        columns = columns.filterNot(_ == "label") // remove label column
        val assembler = new VectorAssembler().setInputCols(columns)
            .setOutputCol("features")
        val lr = new LogisticRegression()
            .setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
            .setLabelCol("label")
        
        lr.fit(training_data)*/

        /*val pipeline = new Pipeline().setStages(Array(assembler,lr))

        lr.fit(assembler.transform(training_data)) */
    }

    /**
    * Prepare data for a regression by changing String columon to StringIndex column
    * @param data: the dataFrame that will be prepared 
    */
    def prepareData (data: DataFrame): Any /*DataFrame*/ = { // TODO
        val appOrSiteIndexer = new StringIndexer().setInputCol("appOrSite").setOutputCol("appOrSiteIndex")
        val cityIndexer = new StringIndexer().setInputCol("city").setOutputCol("cityIndex")
        val exchangeIndexer = new StringIndexer().setInputCol("exchange").setOutputCol("exchangeIndex")
        val impidIndexer = new StringIndexer().setInputCol("impid").setOutputCol("impidIndex")
        val interestsIndexer = new StringIndexer().setInputCol("interests").setOutputCol("interestsIndex")
        val mediaIndexer = new StringIndexer().setInputCol("media").setOutputCol("mediaIndexer")
        val networkIndexer = new StringIndexer().setInputCol("network").setOutputCol("networkIndex")
        val osIndexer = new StringIndexer().setInputCol("os").setOutputCol("osIndex")
        val publisherIndexer = new StringIndexer().setInputCol("publisher").setOutputCol("publisherIndex")
        val sizeIndexer = new StringIndexer().setInputCol("size").setOutputCol("sizeIndex")
        val typeIndexer = new StringIndexer().setInputCol("type").setOutputCol("typeIndex")
        val userIndexer = new StringIndexer().setInputCol("user").setOutputCol("userIndex")

        var columns = data.columns // every column of dataFrame
                    .filterNot(_ == "label") // remove label column
        
        val assembler = new VectorAssembler()
            .setInputCols(columns).setOutputCol("features")

        val lr = new LogisticRegression()

        val pipeline = new Pipeline().setStages(
            Array(appOrSiteIndexer, cityIndexer, exchangeIndexer, impidIndexer, interestsIndexer, 
            mediaIndexer, networkIndexer, osIndexer, publisherIndexer, sizeIndexer, typeIndexer, 
            userIndexer, assembler, lr)
        )

        val Array(training, test) = data.randomSplit(Array(0.8, 0.2), seed=12345)
        val model = pipeline.fit(training)
        val result =  model.transform(test)


        result.printSchema()

        println(s" ${model.explainParams()}")
    }
}
