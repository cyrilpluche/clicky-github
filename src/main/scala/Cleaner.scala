import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, _}

object Cleaner {

  /**
    * clen every columon of the dataframe and return a new data frame
    * @param df: A DataFrame
    * @return
    */
 def clean (df: DataFrame): DataFrame = {
   var newDF = cleanSize(df)
   newDF = cleanLabel(newDF)
   newDF = cleanOS(newDF)
   newDF = cleanInterest(newDF)
   newDF = cleanBidfloor(newDF)
   cleanType(newDF)
  }

  /*private def network (df: DataFrame): DataFrame = {

  }*/

  private def cleanSize (df: DataFrame): DataFrame = {
    df.withColumn("size",
      when(col("size").isNotNull, s"${col("size")(0)} x ${col("size")(1)}")
    )
  }

  private def cleanLabel (df: DataFrame): DataFrame = {
    df.withColumn("label", 
      when(col("label") === "false", 0).otherwise(1)
      // if the column value label is equal to false set value to 0 otherwise set the value to 1
    )
  }

  private def cleanOS (df: DataFrame): DataFrame = { // TODO
      df
  }

  private def cleanBidfloor (df: DataFrame): DataFrame = { // TODO
    df
  }

  private def cleanInterest (df: DataFrame): DataFrame = { // TODO
    df
  }

  private def cleanType (df: DataFrame): DataFrame = { // TODO
    df
  }
}
