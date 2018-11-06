package analysis
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, VectorIndexer, OneHotEncoder, _}

/**
 * @param columnName
 * @param indexerName: The possible name of indexer output colname 
 * @param isStringIndexer: If isStringIndexer == false this means that the column is not String typed
 *  Therefore it's not necessary to create an indexer from this column
 */
case class ColumnIndexer(val columnName: String, isStringIndexer: Boolean = true) {

    def createIndex: Option[StringIndexer] = {
        if (isStringIndexer) Some(
                new StringIndexer().setInputCol(columnName).setOutputCol(getOutPutName)
        )
        else None
    }

   
   /**
    * Return the output 
    * @return String
    */
    def getOutPutName: String = {
        if (!isStringIndexer) columnName
        else columnName + "index"
    }
}
