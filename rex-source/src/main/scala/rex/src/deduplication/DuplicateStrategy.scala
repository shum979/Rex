package rex.src.deduplication

import org.apache.spark.sql.Row
import rex.src.deduplication.impl.{DefaultDuplicateStrategy, DropDuplicatesDuplicateStrategy, MaxDuplicateStrategy, MinDuplicateStrategy}
import rex.src.utils.AppConstants._

import scala.util.{Failure, Success, Try}


trait DuplicateStrategy {
  /**
    * This method is an abstraction of the strategy logic to apply to identify the duplicates on the row sequence
    *
    * @param rowSeq
    * @param strategyField
    * @return
    */
  def deDuplicate(rowSeq: Seq[Row], strategyField: String): Row

  def sortRowsAsPerStrategy(rowSeq: Seq[Row], strategyField: String): Seq[Row]

  /*Custom comparator used to sort rows based on given field*/
  def sortRowBasedOnFieldValue(row1: Row, row2: Row, strategyField: String): Boolean = {
    val leftValue = if (row1 != null && row1.getAs(strategyField) != null)
      Try(row1.getAs(strategyField).toString.toDouble) match {
        case Success(x) => x
        case Failure(x) => 0
      }
    else
      Int.MinValue

    val rightValue = if (row2 != null && row2.getAs(strategyField) != null)
      Try(row2.getAs(strategyField).toString.toDouble) match {
        case Success(x) => x
        case Failure(x) => 0
      }
    else
      Int.MinValue

    leftValue > rightValue
  }
}

object DuplicateStrategy {
  def apply(strategy: String): DuplicateStrategy = {
    strategy.toUpperCase match {
      case MAX_STRATEGY => MaxDuplicateStrategy
      case MIN_STRATEGY => MinDuplicateStrategy
      case DROP_STRATEGY => DropDuplicatesDuplicateStrategy
      case _ => DefaultDuplicateStrategy
    }
  }
}




