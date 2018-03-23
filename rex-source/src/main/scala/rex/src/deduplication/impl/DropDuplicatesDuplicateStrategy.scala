package rex.src.deduplication.impl

import org.apache.spark.sql.Row
import rex.src.deduplication.DuplicateStrategy

/**
  * An implementation of Strategy Type to drop all rows with duplicates
  */
object DropDuplicatesDuplicateStrategy extends DuplicateStrategy {
  /*
  Returns the row only when length of rowSeq is 1.
   */
  override def deDuplicate(rowSeq: Seq[Row], strategyField: String) =
    if (rowSeq.length == 1)
      rowSeq(0)
    else
      Row.empty

  override def sortRowsAsPerStrategy(rowSeq: Seq[Row], strategyField: String): Seq[Row] = {
    rowSeq
  }
}
