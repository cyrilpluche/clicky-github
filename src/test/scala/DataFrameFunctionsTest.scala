import preparation._
import org.scalatest.FunSuite
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import analysis._

class DataFrameFunctionsTest extends FunSuite {
    
    
    test ("getPercentageNullValue is set correctly") {
        Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession.builder().config("spark.master","local").getOrCreate()

        import spark.implicits._

        val NO_VALUE = "no-value"

         val someDF = Seq(
            (8, "bat", null),
            (64, "mouse", NO_VALUE),
            (-27, "horse", null),
            (12, NO_VALUE, null)
        ).toDF("number", "word", "nullCol")

        assert(DataFrameFunctions.getPercentageNullValue(someDF, "word") == 25.0)
        assert(DataFrameFunctions.getPercentageNullValue(someDF, "nullCol") == 100.0)
    }
    
    test("dropNonRelevantColumns is set correctly") {
        
         Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession.builder().config("spark.master","local").getOrCreate()

        import spark.implicits._

        val someDF = Seq(
            (8, "bat"),
            (64, "mouse"),
            (-27, "horse")
        ).toDF("number", "word")


         val someDF1 = Seq(
            (8, "bat", null),
            (64, "mouse", null),
            (-27, "horse", null)
        ).toDF("number", "word", "nullCol")


        assert(DataFrameFunctions.dropNonRelevantColumns(someDF).columns.length == 2)
    }

    test("RandomSplit is set correctly") {
        Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession.builder().config("spark.master","local").getOrCreate()

        import spark.implicits._

        val someDF = Seq(
            (1),(1), (0), (1), (0),
            (0), (1), (1), (0), (0),
            (1),(1), (0), (1), (0),
            (0), (1), (1), (0), (0)
        ).toDF("label")

        val Array(train, test) = DataFrameFunctions.randomSplit(someDF, seed = 12345)
        assert(train.count >= test.count)
    }
}