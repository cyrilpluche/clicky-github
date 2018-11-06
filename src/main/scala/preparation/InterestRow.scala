package preparation
import scala.io.Source
case class InterestRow (val code: String, val label: String)

/**
compagnion object 
*/
object InterestRow {
    val DEFAULT_DELIMITER = ","
    def interestRowsFromCSV(path: String, delimiter: String = DEFAULT_DELIMITER): List[InterestRow] = {
        val bfSource = Source.fromFile(path)
        var listRows: List[InterestRow] = Nil
        for (lines <- bfSource.getLines.drop(1)) {
            val cols = lines.split(delimiter).map(_.trim)
            listRows = InterestRow(cols(0), cols(1)) ::listRows
        }
        bfSource.close
        listRows
    }
}