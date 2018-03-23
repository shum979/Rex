package rex.src.deduplication.impl

import org.apache.spark.sql.Row
import rex.src.deduplication.DuplicateStrategy

/**
  * A default implementation of Strategy Type
  */
object DefaultDuplicateStrategy extends DuplicateStrategy {


  /* Returns the first row in rowSeq. */
  override def deDuplicate(rowSeq: Seq[Row], strategyField: String) = rowSeq(0)

  override def sortRowsAsPerStrategy(rowSeq: Seq[Row], strategyField: String): Seq[Row] = {
    rowSeq
  }
}
