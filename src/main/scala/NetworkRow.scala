
import scala.io.Source
case class NetworkRow(val code: String, val brand: String, val operator: String) {
    def getBrandOrOperator: String = {
        if (brand == "NO_BRAND") operator
        else brand
    }
}


/**
compagnion object 
*/
object NetworkRow {
    val DEFAULT_DELIMITER = ","
    def newtWorkRowsFromCSV(path: String, delimiter: String = DEFAULT_DELIMITER): List[NetworkRow] = {
        val bfSource = Source.fromFile(path)
        var listRows: List[NetworkRow] = Nil
        for (lines <- bfSource.getLines.drop(1)) { // drop the title row
            val cols = lines.split(delimiter).map(_.trim)
            listRows = NetworkRow(cols(0), cols(1), cols(2)) ::listRows
        }
        bfSource.close
        listRows
    }
}