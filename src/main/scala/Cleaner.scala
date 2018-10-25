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
 def clean (df: DataFrame, spark: SparkSession): DataFrame = {
  import spark.implicits._

   var newDF = cleanSize(df)
   newDF = cleanLabel(newDF)
   newDF = cleanOS(newDF)
   newDF = cleanInterest(newDF)
   newDF = cleanBidfloor(newDF)
   newDF = cleanNetwork(newDF)
   cleanType(newDF)
  }

  
  private def cleanSize (df: DataFrame): DataFrame = {

    df.withColumn("size",
      when(col("size").isNotNull, 
      concat(col("size")(0).cast("STRING"), lit("x"), col("size")(1).cast("STRING"))
      )
    )
    /* Function comming from the org.apache.spark.sql.functions
    * withColumn: Create a new column with as name the first parameter 
    * (or replace if a column with the same name exists)
    * contact: contact several column into a new one 
    * lit: create a new column with the value given in parameter of the function
    */
  }

  private def cleanLabel (df: DataFrame): DataFrame = {
    df.withColumn("label", 
      when(
        col("label") === "false", 
        0).otherwise(1)
      // if the column value label is equal to false set value to 0 otherwise set the value to 1
    )
  }

  private def cleanOS (df: DataFrame): DataFrame = { 
      val list_os = List("amazon", "windows", "android", "ios")
        // TODO maybe add some os to this list
      def cleanOSint (dframe: DataFrame, list: List[String]): DataFrame = {
        if (list.isEmpty) dframe
        else {
          val df = dframe.withColumn("os",
              when(
                  lower(col("os")).contains(list.head),
                  list.head.capitalize // first letter in upperCase 
                ).otherwise(col("os"))
            )

          cleanOSint(df, list.tail)
        }
      }

      cleanOSint(df, list_os)
  }


  private def cleanBidfloor (df: DataFrame): DataFrame = { // TODO
    // fill empty values with average of the bidfloor column
    val averageDF = df.select(mean(df("bidfloor"))).collect()(0).getDouble(0)

    val d = mean(df.col("bidfloor")).getItem(0)
        df.withColumn("bidfloor", 
      when(
        col("bidfloor").isNull or col("bidfloor").isNaN, 
        averageDF
      ).otherwise(col("bidfloor"))
    
    )
  }

  private def cleanInterest (df: DataFrame): DataFrame = { // TODO
    df
  }

  private def cleanType (df: DataFrame): DataFrame = { // TODO
    df.withColumn("type",
      when(
        col("type") === 1 or col("type") === 2 or col("type") === 3 or col("type") === 4,
        col("type")
      ).otherwise("no-value")
    
    )
  }

  private def cleanNetwork (df: DataFrame): DataFrame = {
    df
  }
}
