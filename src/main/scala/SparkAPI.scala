import org.apache.spark._
import org.apache.log4j._

/**
  * Created by toddmcgrath on 6/15/16.
  */
object SparkAPI {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "ScalaProcess")
    val lines = sc.textFile("public/data-students.json")
    val output = lines.take(10)
    sc.stop()

    println(output(0))
    println(output(1))
    println(output(3))

  }

}
