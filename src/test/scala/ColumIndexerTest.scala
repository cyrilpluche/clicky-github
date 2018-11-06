import org.scalatest.FunSuite
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import analysis._

class ColumnIndexerTest extends FunSuite {

    test("Can create a DataFrame") {
        Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession.builder().config("spark.master","local").getOrCreate()

        import spark.implicits._

        val someDF = Seq(
            (8, "bat"),
            (64, "mouse"),
            (-27, "horse")
        ).toDF("number", "word")

        assert(someDF.isInstanceOf[DataFrame])
    }


    test ("Is Array") {
         Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession.builder().config("spark.master","local").getOrCreate()

        import spark.implicits._

        val someDF = Seq(
            (8, "bat"),
            (64, "mouse"),
            (-27, "horse")
        ).toDF("number", "word")

        val arr = someDF.columns

        assert(arr.isInstanceOf[Array[String]])
    }


    test ("ColumnIndexer getOutPutName") {
         Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession.builder().config("spark.master","local").getOrCreate()

        import spark.implicits._

        val someDF = Seq(
            (8, "bat"),
            (64, "mouse"),
            (-27, "horse")
        ).toDF("number", "word")

        val cols = someDF.columns
        val cIndex1 =  ColumnIndexer(cols(0), false)
        val cIndex2 = ColumnIndexer(cols(1))

        assert(cIndex1.getOutPutName.equals("number"))
        assert(!cIndex2.getOutPutName.equals("word"))
        assert(cIndex2.getOutPutName.equals("wordindex"))
    }

    test("ColumnIndexer createIndex") {
        Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession.builder().config("spark.master","local").getOrCreate()

        import spark.implicits._

        val someDF = Seq(
            (8, "bat"),
            (64, "mouse"),
            (-27, "horse")
        ).toDF("number", "word")

        val cols = someDF.dtypes
        val cIndex1 =  ColumnIndexer(cols(0)._1, (cols(0)._2).equals("StringType"))
        val cIndex2 = ColumnIndexer(cols(1)._1, (cols(1)._2).equals("StringType"))
        
        assert(cIndex1.createIndex.isEmpty)
        assert(!cIndex2.createIndex.isEmpty)
    }
}