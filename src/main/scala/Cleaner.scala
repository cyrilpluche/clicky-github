import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, _}
import scala.annotation.tailrec

object Cleaner {
  val NO_VALUE = "no-value"
  /**
    * clen every columon of the dataframe and return a new data frame
    * @param df: A DataFrame
    * @return
    */
 def clean (df: DataFrame, spark: SparkSession): DataFrame = {
  import spark.implicits._
    println(s"\n${Console.UNDERLINED}${Console.RED}Sart cleaning data :${Console.RESET}\n")
   var newDF = cleanNetwork(df)
   println(s"-\tNetwork : ${Console.BLUE}${Console.BOLD}Finished${Console.RESET}")
   newDF = cleanLabel(newDF)
   println(s"-\tLabel : ${Console.BLUE}${Console.BOLD}Finished${Console.RESET}")
   newDF = cleanOS(newDF)
   println(s"-\tFinish OS : ${Console.BLUE}${Console.BOLD}Finished${Console.RESET}")
   newDF = cleanInterest(newDF)
   println(s"-\tInterest : ${Console.BLUE}${Console.BOLD}Finished${Console.RESET}")
   newDF = cleanBidfloor(newDF)
   println(s"-\tBidFloor : ${Console.BLUE}${Console.BOLD}Finished${Console.RESET}")
   newDF = cleanSize(newDF)
   println(s"-\tSize : ${Console.BLUE}${Console.BOLD}Finished${Console.RESET}")
   newDF = cleanType(newDF)
   println(s"-\tType : ${Console.BLUE}${Console.BOLD}Finished${Console.RESET}")
   newDF
  }


 private def cleanNetwork (df: DataFrame): DataFrame = {
    val list: List[NetworkRow] = NetworkRow.newtWorkRowsFromCSV("public/network.csv")
    var dframe = df.withColumn("network",
      when(
        col("network") === "208-1", "208-01"
      ).otherwise(col("network"))
    ) // replace 208-1 by 208-01
    .withColumn("network",
      when( // replace the null value
        col("network").isNull, NO_VALUE
      ).otherwise(col("network"))
    )

    /*for (row <- list) {
      print(row.code)
      dframe = dframe.withColumn(
            "network",
            when(
              col("network") === row.code,
              row.getBrandOrOperator
            ).otherwise(col("network"))
        )
        print("End\n")
    }
    println("End")
    return dframe */
    def cleanNetworkInt (dataF: DataFrame, netRows: List[NetworkRow]): DataFrame = {
      if (netRows.isEmpty) dataF
      else {
        val d = dataF.withColumn(
            "network",
            when(
              col("network") === netRows.head.code,
              netRows.head.getBrandOrOperator
            ).otherwise(col("network"))
        )
        cleanNetworkInt(d, netRows.tail)
      }
    }
    cleanNetworkInt(dframe, list)
    
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


  private def cleanBidfloor (df: DataFrame): DataFrame = { 
    // fill empty values with average of the bidfloor column
    val averageDF = df.select(mean(df("bidfloor"))).collect()(0).getDouble(0)
      // get the average value 

    val d = mean(df.col("bidfloor")).getItem(0)
        df.withColumn("bidfloor", 
      when(
        col("bidfloor").isNull or col("bidfloor").isNaN, 
        averageDF
      ).otherwise(col("bidfloor"))
    
    )
  }

  private def cleanInterest (df: DataFrame): DataFrame = { // TODO
    /*val list: List[InterestRow] = InterestRow.interestRowsFromCSV("public/interest.csv")
    val dataframe = df.withColumn("interests",
      when(
        col("interests").isNotNull, col("interests")
      ).otherwise(NO_VALUE)
    )*/

    // use map dataframe.select(interests).map here 
    /* val listRows: List[Row] = dataframe.select("inerests").collectAsList().map(r => {
      r.getString(0).split(",")
        TODO replace the value with value of list if they match 
        * then contact every part to get a string 
        
    })*/
    df
  }

  private def cleanType (df: DataFrame): DataFrame = {
    df.withColumn("type",
      when(
        col("type") === 1 or col("type") === 2 or col("type") === 3 or col("type") === 4,
        col("type")
      ).otherwise(NO_VALUE)
    
    )
  }

 
}
