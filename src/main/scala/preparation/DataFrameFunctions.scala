package preparation
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, _}

object DataFrameFunctions {
  val NO_VALUE = "no-value"

  /**
  * @param df: A dataFrame
  * @param acceptanceThreshold: The threshold over which a column will be dropped 
  * Drop all the column of the dataframe that has more than acceptanceThreshold % of Null value
  */
   def dropNonRelevantColumns (df: DataFrame, acceptanceThreshold: Double = 25): DataFrame = {

    def dropNonRelevantFromArray (df: DataFrame, colNames: Array[String], threshold: Double, 
      index: Int): DataFrame = {
        if (colNames.length == index) df
        else {
              val percentageNullValue = getPercentageNullValue(df, colNames(index))
              if (percentageNullValue > threshold) { // the percentage 
                val d = df.drop(col(colNames(index)))
                dropNonRelevantFromArray(d, colNames, threshold, index + 1)
              } else dropNonRelevantFromArray(df, colNames, threshold, index + 1)
        }
      }
      dropNonRelevantFromArray(df, df.columns, acceptanceThreshold, 0) 
  }

  /**
  * Get the percentage of null or no-alue of the column
  * @param df: A DataFrame
  * @param colName: The column the method will process on
  */
  def getPercentageNullValue (df: DataFrame, colName: String): Double = {
    val dfSize = df.select(colName).count
    val nbNullValue = df.filter( col(colName).equalTo(NO_VALUE) or col(colName).isNull).select(colName).count

    (nbNullValue * 100)/ dfSize
  }

  /**
  * Custom random split 
  * Split the label by taking care of the 1 and 0 value 
  * There will be the same percentage of 1 and 0 than in the initial dataframe 
  * @param df: the DataFrame that will be split 
  * @param weights: The weight of each part 
  * @param seed: A seed to perform the random Split
  */
  def randomSplit(df: DataFrame, weights: Array[Double] = Array(0.7, 0.3), seed: Long = 1L): Array[DataFrame] = {
    val positiveValueDf = df.filter(col("label").equalTo(1))
    val negativeValueDf = df.filter(col("label").equalTo(0))
   
    positiveValueDf.select("label").show(10)
    val Array(trainP, testP) = positiveValueDf.randomSplit(weights, seed)
    val Array(trainN, testN) = negativeValueDf.randomSplit(weights, seed)
    Array (trainP.unionByName(trainN), testP.unionByName(testN))
  }

  def prepareCSV(df: DataFrame): DataFrame = {
    def prepareCsvFromArray (d: DataFrame, columns: Array[String], index: Int): DataFrame = {
      if (columns.length == index) d
      else {
        val newDF = d.withColumn(columns(index), col(columns(index)).cast("String"))
        prepareCsvFromArray(newDF, columns, index + 1)
      }
    }

    prepareCsvFromArray(df, df.columns, 0)
  }
}