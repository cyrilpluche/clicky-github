import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, _}
import scala.annotation.tailrec

object Training {
  val NO_VALUE = "no-value"
  /**
    * clen every columon of the dataframe and return a new data frame
    * @param df: A DataFrame
    * @return
    */
 def cleanData (df: DataFrame, spark: SparkSession): DataFrame = {
   import spark.implicits._

   print(s"\n${Console.UNDERLINED}${Console.RED}Sart cleaning data :${Console.RESET}\n\n-\tInterest : ")

   var newDF = cleanInterest(df, spark)
   print(s"${Console.BLUE}${Console.BOLD}Finished${Console.RESET}\n-\tNetwork : ")

   newDF = cleanNetwork(newDF)
   print(s"${Console.BLUE}${Console.BOLD}Finished${Console.RESET}\n-\tLabel : ")
   newDF = cleanLabel(newDF)
   print(s"${Console.BLUE}${Console.BOLD}Finished${Console.RESET}\n-\tOS : ")
   newDF = cleanOS(newDF)
   print(s"${Console.BLUE}${Console.BOLD}Finished${Console.RESET}\n-\tBidFloor : ")
  
   newDF = cleanBidfloor(newDF)
   print(s"${Console.BLUE}${Console.BOLD}Finished${Console.RESET}\n-\tSize : ")
   newDF = cleanSize(newDF)
   print(s"${Console.BLUE}${Console.BOLD}Finished${Console.RESET}\n-\tType : ")
   newDF = cleanType(newDF)
   println(s"${Console.BLUE}${Console.BOLD}Finished${Console.RESET}")
   newDF
  }


 private def cleanNetwork (df: DataFrame): DataFrame = {
    val listFromCSV: List[NetworkRow] = NetworkRow.newtWorkRowsFromCSV("public/network.csv")
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
    cleanNetworkInt(dframe, listFromCSV)
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
        col("label") === "true", 
        1).otherwise(0)
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

  private def cleanInterest (df: DataFrame, spark: SparkSession): DataFrame = { // TODO
    import spark.implicits._
    /**
    * @param oldValue: the value we need to replace 
    * @param interestRowsList: List of InterestRow @see InterestRow
    * Replace return the label corresponding to the oldValue.
    * oldValue is considerated as a code, an interestRow is composed of a code and a label
    * if the oldValue is matching the code we send back the label associated 
    **/
    def replaceValue (oldValue: String, interestRowsList: List[InterestRow]): String = {
      if (interestRowsList.isEmpty) oldValue
      else if (oldValue == interestRowsList.head.code) interestRowsList.head.label
      else replaceValue(oldValue, interestRowsList.tail)
    }


    def processReplacement (dataf: DataFrame, col: Column, interestRowsList: List[InterestRow]): String = {
        val oldValue = dataf.select(col).collect()(0).getString(0)
        println("Old value " +  oldValue + "  size " +  
          dataf.select(col).collect().length)
        replaceValue(oldValue, interestRowsList)
    }

    def processRegexReplacement (dataF: DataFrame, interestRowsList: List[InterestRow]): DataFrame = {
      if (interestRowsList.isEmpty) dataF 
      else {
        val d = dataF.withColumn("interests",
          when(
            col("interests").isNotNull,
            regexp_replace(col("interests"), interestRowsList.head.code, interestRowsList.head.label)
          ).otherwise(NO_VALUE)
        )
       processRegexReplacement(d, interestRowsList.tail)
      }
    }


    val listRowFromCSV: List[InterestRow] = InterestRow.interestRowsFromCSV("public/interest.csv")

    processRegexReplacement(df, listRowFromCSV)

    /*val d = df.withColumn("interests",
      when(
        col("interests").isNull, NO_VALUE
      ).otherwise(col("interests"))
    )
    .withColumn("interests",
      when(
        col("interests").notEqual(NO_VALUE),
        processReplacement(df, col("interests"), listRowFromCSV)
      ).otherwise(col("interests"))
    )

    d.select("interests").show()
    d
    val dataframe = df.withColumn("interests",
      when(
        col("interests").isNotNull, col("interests")
      ).otherwise(NO_VALUE)
    ) */

  
     /*var d = df.select("interests").map(row => { 
      // map on the column interests the column will be named value
      if (row.anyNull) NO_VALUE
      else {
        row.getString(0).split(",").map(value => { // replace the value by the value of the list
          replaceValue(value, listRowFromCSV)
        }).mkString(",") // join all the cell of the array together separated by a ','
      }
    }).asInstanceOf[DataFrame]
    
    d = d.withColumnRenamed("value", "interests")
    d.select("interests").show()
  
    d.cache()
  
    //dataframe.cache()
    df.select("*").withColumn("interest", 
      d.col("interests")
    ).toDF()
    // replace the column by the new computed column */
    
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
