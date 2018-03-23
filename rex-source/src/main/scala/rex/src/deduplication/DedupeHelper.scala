package rex.src.deduplication

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{IntegerType, StringType, StructField}
import rex.src.utils.AppConstants._


/**
  * This object marks the duplicate rows belonging to a dataset according to the strategy and strategy field specified.
  */
object DedupeHelper {
  /*
  Filter the rows based on Strategy Value and Columns.
   */
  def filterDuplicates(rowSeq: Seq[Row], strategyField: String, strategy: String): Seq[Row] = {
    val rowSchema = rowSeq(0).schema.add(StructField(IS_DUPLICATE_COLUMN, StringType)).add(StructField(DUPLICATE_COUNT_COLUMN, IntegerType, false))
    if (rowSeq.length > 1) {
      val strategyRow = DuplicateStrategy(strategy).deDuplicate(rowSeq, strategyField)
      rowSeq.map { row: Row => {
        new GenericRowWithSchema(Row.fromSeq(row.toSeq :+ (if (strategyRow.eq(row)) IS_DUPLICATE_VALUE_NOT_DUPLICATE else IS_DUPLICATE_VALUE_DUPLICATE) :+ rowSeq.length).toSeq.toArray, rowSchema)
      }
      }
    } else {
      rowSeq.map { row: Row => {
        new GenericRowWithSchema(Row.fromSeq(row.toSeq :+ IS_DUPLICATE_VALUE_NOT_DUPLICATE :+ 1).toSeq.toArray, rowSchema)
      }
      }
    }
  }


  def getUniqueRowBasedOnStrategy(rowSeq: Seq[Row], strategyField: String, strategy: String): Row = {

    if (rowSeq.length > 1) {
      val duplicateStrategy = DuplicateStrategy(strategy)
      val strategyRow: Row = duplicateStrategy.deDuplicate(rowSeq, strategyField)
      strategyRow
    }
    else {
      rowSeq(0)
    }
  }

  def getSortedRowBasedOnStrategy(rowSeq: Seq[Row], strategyField: String, strategy: String): Seq[Row] = {
    val duplicateStrategy = DuplicateStrategy(strategy)
    duplicateStrategy.sortRowsAsPerStrategy(rowSeq, strategyField)
  }
}

/**
  * This is a wrapper case class over the DeDuplicationType case class
  *
  * @param groupingKeys
  * @param strategy
  * @param strategyField
  */
case class DeduplicateCaseClass(groupingKeys: Seq[String], strategy: String, strategyField: String)

