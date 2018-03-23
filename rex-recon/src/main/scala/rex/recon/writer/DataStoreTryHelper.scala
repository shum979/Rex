package rex.recon.writer

import org.apache.spark.sql.Row
import rex.src.utils.AppConstants._

/**
  * Created by vrai on 2/4/2018.
  */
/**
  * This is a helper class to write duplicates to a location.  *
  */
trait DataStoreTryHelper {

  /**
    * This method identifies the duplicates in the iterator of Row and writes it using the DataHandler
    *
    * @param itr
    * @param datahandler
    * @param func
    * @return
    */
  def withDataStoreHelperOnRowIterator(itr: Iterator[Row], datahandler: DataHandler)(func: Row => Row) = {
    var duplicateRowList: List[Row] = Nil
    var notDuplicateRowList: List[Row] = Nil
    while (itr.hasNext) {
      val row = itr.next()
      if (row != null && row.length > 0) {
        if (row.getAs[String](IS_DUPLICATE_COLUMN) != null && row.getAs[String](IS_DUPLICATE_COLUMN).equalsIgnoreCase(IS_DUPLICATE_VALUE_DUPLICATE)) {
          duplicateRowList = duplicateRowList.::(row)
        }
        else {
          notDuplicateRowList = notDuplicateRowList.::(row)
        }
      }
    }
    if (duplicateRowList != Nil) {
      datahandler.writedata(duplicateRowList)
      datahandler.closeWriter()
    }
    notDuplicateRowList.toIterator
  }
}
