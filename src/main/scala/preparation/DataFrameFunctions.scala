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
  * Get the percentage of null Value of the column
  * @param df: A DataFrame
  * @param colName: The column the method will process on
  */
  def getPercentageNullValue (df: DataFrame, colName: String): Double = {
    val dfSize = df.select(colName).count
    val nbNullValue = df.filter( col(colName).equalTo(NO_VALUE) or col(colName).isNull).select(colName).count

    (nbNullValue * 100)/ dfSize
  }
}