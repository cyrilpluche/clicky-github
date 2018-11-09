package preparation
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, _}
import scala.annotation.tailrec

object DataCleaner {
  val NO_VALUE = "no-value"
  /**
    * clen every columon of the dataframe and return a new data frame
    * @param df: A DataFrame
    * @return
    */
 def clean (df: DataFrame, spark: SparkSession): DataFrame = {
   import spark.implicits._
 
    // TODO remove null value from every columns 
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
   print(s"${Console.BLUE}${Console.BOLD}Finished${Console.RESET}\n-\tImpid : ")

   newDF = cleanImpid(newDF)
   print(s"${Console.BLUE}${Console.BOLD}Finished${Console.RESET}\n-\tSize : ")

   newDF = cleanSize(newDF)
   print(s"${Console.BLUE}${Console.BOLD}Finished${Console.RESET}\n-\tType : ")

   newDF = cleanType(newDF)
   print(s"${Console.BLUE}${Console.BOLD}Finished${Console.RESET}\n-\tCity : ")

   newDF = cleanCity(newDF)
   print(s"${Console.BLUE}${Console.BOLD}Finished${Console.RESET}\n-\tTimeStamp : ")

   newDF = cleanTimeStamp(newDF)

   println(s"${Console.BLUE}${Console.BOLD}Finished${Console.RESET}")

    cleanNullValue(newDF)
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
    def cleanNetworkInt (netWorkCol: Column, netRows: List[NetworkRow]): Column = {
      if (netRows.isEmpty) netWorkCol//dataF
      else {
        /*val d = dataF.withColumn(
            "network",
            when(
              col("network") === netRows.head.code,
              netRows.head.getBrandOrOperator
            ).otherwise(col("network"))
        ) */

        val col = regexp_replace(netWorkCol, netRows.head.code, netRows.head.getBrandOrOperator)
        cleanNetworkInt(col, netRows.tail)
      }
    }
    dframe.withColumn("network",
      cleanNetworkInt(col("network"), listFromCSV))
  }
  
  private def cleanSize (df: DataFrame): DataFrame = {

    df.withColumn("size",
      when(col("size").isNotNull, 
      concat(col("size")(0).cast("STRING"), lit("x"), col("size")(1).cast("STRING"))
      ).otherwise(NO_VALUE)
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
        1.0).otherwise(0.0)
      // if the column value label is equal to false set value to 0 otherwise set the value to 1
    )
  }

  private def cleanOS (df: DataFrame): DataFrame = { 
      val list_os = List("amazon", "windows", "android", "ios")
        // TODO maybe add some os to this list
      val dframe = df.withColumn("os",
        when(col("os").isNull, NO_VALUE).otherwise(col("os"))
      )
      def cleanOSint (dframe: DataFrame, list: List[String]): DataFrame = {
        if (list.isEmpty) dframe
        else {
          val d = dframe.withColumn("os",
              when(
                  lower(col("os")).contains(list.head),
                  list.head.capitalize // first letter in upperCase 
                ).otherwise(col("os"))
            )

          cleanOSint(d, list.tail)
        }
      }

      cleanOSint(dframe, list_os)
  }


  private def cleanBidfloor (df: DataFrame): DataFrame = { 
    // fill empty values with average of the bidfloor column
    val averageDF = df.select(mean(df("bidfloor"))).first()(0).asInstanceOf[Double]
      // get the average value 

    /*val d = mean(df.col("bidfloor")).getItem(0)
        df.withColumn("bidfloor", 
      when(
        col("bidfloor").isNull or col("bidfloor").isNaN, 
        averageDF
      ).otherwise(col("bidfloor")) 

    ) */
      df.na.fill(averageDF, Seq("bidfloor"))

  }


  /**
  * Triggers a "grows beyond 64 KB" error that doesn't stop the program
   */
  private def cleanInterest (df: DataFrame, spark: SparkSession): DataFrame = {
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

    def processRegexReplacement (interestColumn: Column, interestRowsList: List[InterestRow]): Column = {
      if (interestRowsList.isEmpty) interestColumn //dataF 
      else {
        /*val d = dataF.withColumn("interests",
          when(
            col("interests").isNotNull,
            regexp_replace(col("interests"), interestRowsList.head.code, interestRowsList.head.label)
          ).otherwise(NO_VALUE)
        )*/
        val col = regexp_replace(interestColumn, interestRowsList.head.code, interestRowsList.head.label)
        processRegexReplacement(col, interestRowsList.tail)
      }
    }


    val listRowFromCSV: List[InterestRow] = InterestRow.interestRowsFromCSV("public/interest.csv")
    val d = df.withColumn("interests",
      when(
        col("interests").isNull,
        NO_VALUE
      ).otherwise(col("interests"))
    )


    val (part, part0) = listRowFromCSV.splitAt(listRowFromCSV.size / 2)
      // split the list of interest into two sublist to avoid error "gros beyond 64KB"
    
    val (part1, part2) = part.splitAt(part.size / 2)
    val (part3, part4) = part0.splitAt(part0.size / 2)

    val (part5, part6) = part1.splitAt(part1.size / 2)
    val (part7, part8) = part2.splitAt(part2.size / 2)
    val (part9, part10) = part3.splitAt(part3.size / 2)
    val (part11, part12) = part4.splitAt(part4.size / 2) 

    var column = processRegexReplacement(d.col("interests"), part5)
    column = processRegexReplacement(column, part6)
    column = processRegexReplacement(column, part7)
    column = processRegexReplacement(column, part8)
    column = processRegexReplacement(column, part9)
    column = processRegexReplacement(column, part10)
    column = processRegexReplacement(column, part11)
    column = processRegexReplacement(column, part12)
    

    d.withColumn("interests", column)
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
        col("type").isNotNull and
        (col("type") === 1 or col("type") === 2 or col("type") === 3 or col("type") === 4),
        col("type").cast("Double")
      ).otherwise(-1).cast("Double") /* .cast("Double")
      cast the column into double type because the model definition only supports 
      double type 
      */
    ).na.fill(-1, Seq("type"))
  }


  private def cleanImpid (df: DataFrame): DataFrame = {
    df.withColumn("impid",
      when(col("impid").isNull, NO_VALUE).otherwise(col("impid"))
    )
  }

  private def cleanCity (df: DataFrame): DataFrame = {
    df.withColumn("city",
      when(col("city").isNull, NO_VALUE).otherwise(col("city"))
    )
  }

  private def cleanTimeStamp (df: DataFrame): DataFrame = {
    val avg = df.select(
      bround(mean(df("timestamp")))
      ).first()(0).asInstanceOf[Double]

    df.withColumn("timestamp", col("timestamp").cast("Double"))
    .na.fill(avg, Seq("timestamp"))
  }

  private def cleanNullValue (df: DataFrame): DataFrame = {
      df.na.fill(NO_VALUE)
  }


  
 
}
