package rex.src.deduplication

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import rex.core.common.ManagedDataset
import rex.src.utils.AppConstants._

/**
  * Created by Shubham A Gupta on 1/22/2018.
  */

/**
  * Deduplicate is a class to apply de-duplication on a dataset to identify the duplicates according to a strategy
  * applied on a strategy field given in the XML.
  * It takes a case class to fetch groupingKeys, strategy and strategyField
  *
  * @param deduplicateCaseClass
  */
class Deduplicator(deduplicateCaseClass: DeduplicateCaseClass) extends Serializable {

  val groupingKeys = deduplicateCaseClass.groupingKeys
  val strategy = deduplicateCaseClass.strategy
  val strategyField = deduplicateCaseClass.strategyField


  /**
    * this method deduplicate given rows from given managedDataset
    *
    * @param dataset
    * @return
    */
  def deduplicate(dataset: ManagedDataset): ManagedDataset = {
    validateColumns(dataset)
    deduplicateRecords(dataset, groupingKeys, strategy, strategyField)
  }


  /** this method validates if incoming grouping columns exists in given managed dataset */
  private def validateColumns(dataset: ManagedDataset) = {
    val allColumns = dataset.columns()
    val schema: StructType = dataset.schema()
    val groupCols = groupingKeys.toSet.toSeq.flatMap { (colName: String) =>
      val cols = allColumns.filter(col => col == colName)
      if (cols.isEmpty) {
        throw new IllegalArgumentException(
          s"""Cannot resolve column name "$colName" among (${schema.fieldNames.mkString(", ")})""")
      }
      cols
    }
  }


  /** This method filters duplicates on the basis of */
  private def deduplicateRecords(data: ManagedDataset, groupingKeys: Seq[String], strategy: String, strategyField: String): ManagedDataset = {
    strategy match {
      case NO_ACTION_STRATEGY => data
      case _ => {
        val keyValueDS: KeyValueGroupedDataset[String, Row] = data.groupByKey(row => row.getValuesMap(groupingKeys).values.mkString("-"))(Encoders.STRING)
        val schema = data.schema
        val rowEncoder: ExpressionEncoder[Row] = RowEncoder.apply(schema.add(StructField(IS_DUPLICATE_COLUMN, StringType, false)).add(StructField(DUPLICATE_COUNT_COLUMN, IntegerType, false)))

        val finalDs: Dataset[Row] = keyValueDS.flatMapGroups((key: String, rowItr: Iterator[Row]) => {
          val seq: Seq[Row] = rowItr.toSeq
          DedupeHelper.filterDuplicates(seq, strategyField, strategy)

        })(rowEncoder).filter(row => row != Row.empty)
        ManagedDataset(finalDs, data.metadata)
      }
    }
  }
}
