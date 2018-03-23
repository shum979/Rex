package rex.recon.newimplement

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, Row}
import rex.recon.helper.{FieldLevelDiff, ReconConfigDataHolder, ReconUtil}
import rex.src.deduplication.{DeduplicateCaseClass, DuplicateStrategy}
import rex.src.utils.AppConstants._

import scala.collection.mutable.ListBuffer
import scala.util.Try


/**
  * Created by visrai on 2/24/2018.
  * 1. cogroup between two KeyValueGroupedDataset
  * 2. Create a seq[Row] and add below records into it
  * 3. populate leftmissing and right duplicate records
  * 4. populate rightmissing and left duplicate records
  * 5. populate reconcilied row and left and right dup records
  */
class WithCogroupImplementation(reconConfigDataHolder: ReconConfigDataHolder) extends Serializable {

  /**
    *
    * @param leftGroupedDataSet
    * @param rightGroupedDataSet
    * @return
    * take two KeyValueGroupedDataset for left and right side as input
    * do cogroup between these two grouped dataset
    * left and right dataset schema merged together along with field-level difference to create cogrouped dataset[Row] final schema
    * return would be final dataset[Row] with combined flattern structure from left and right side and rowstatus as {MATCH, BREAK, MISSING_ON_RIGHT, MISSING_ON_LEFT,DUPLICATE_ONL_EFT, DUPLICATE_ON_RIGHT}
    */

  def doCoGroup(leftGroupedDataSet: KeyValueGroupedDataset[String, Row], rightGroupedDataSet: KeyValueGroupedDataset[String, Row]): Dataset[Row] = {

    val combinedSchema: StructType = ReconUtil.createCombinedSchemaAfterMerging(reconConfigDataHolder.leftDataSetSchema,
      reconConfigDataHolder.rightDataSetSchema)
    val rowEncoder = RowEncoder(combinedSchema)

    leftGroupedDataSet.cogroup(rightGroupedDataSet)(applyRecon)(rowEncoder)
  }


  /*------------------Helper utility class to check if sequence is not null and empty --------------------------------*/
  private implicit class SequenceOps[A](sequence: Seq[A]) {
    def hasNoElements: Boolean = Try(sequence.length).isSuccess && sequence.isEmpty
  }

  /**
    * @param key
    * @param itr1
    * @param itr2
    * @return iterator
    *         Cogroup iterator method applied on key , iterator of left side record, iterator of right side record
    *         this method use to find records
    *  1. Missing from left side that is present only at right side => one record will mark as missing_on_left and rest will mark as duplicate_on_right
    *  2. Missing from right side that is present only at left side => one record will mark as missing_on_right and rest will mark as duplicate_on_left
    *  3. Matching key found from both side => one record will mark as reconcilied row and rest will mark as duplicate_on_left and duplicate_on_right
    *
    */

  def applyRecon(key: String, itr1: Iterator[Row], itr2: Iterator[Row]) = {

    val leftSeq = itr1.toSeq
    val rightSeq = itr2.toSeq
    val resultingList = new ListBuffer[Row]()

    if (leftSeq.hasNoElements) {
      val duplicateSortedRows = treatDuplicateAsPerStrategy(reconConfigDataHolder.rightDedupCaseClass, rightSeq)

      resultingList += fullRowSchemaWithStatus(duplicateSortedRows.head, MISSING_LEFT)
      resultingList ++= duplicateSortedRows.tail.map(row => fullRowSchemaWithStatus(row, RHS_DUPLICATE))
    }
    else if (rightSeq.hasNoElements) {
      val duplicateSortedRows = treatDuplicateAsPerStrategy(reconConfigDataHolder.leftDedupCaseClass, leftSeq)
      resultingList += fullRowSchemaWithStatus(duplicateSortedRows.head, MISSING_RIGHT)
      resultingList ++= duplicateSortedRows.tail.map(row => fullRowSchemaWithStatus(row, LHS_DUPLICATE))
    }
    else {

      val rightStrategyRow = treatDuplicateAsPerStrategy(reconConfigDataHolder.rightDedupCaseClass, rightSeq)
      val leftStrategyRow = treatDuplicateAsPerStrategy(reconConfigDataHolder.leftDedupCaseClass, leftSeq)

      /*First record will use for reconciliation */
      val reconciledRow = doRowRecon(leftStrategyRow.head, rightStrategyRow.head)(reconConfigDataHolder)
      /*Except first record rest will mark as duplicate from both side*/
      if (reconciledRow != Row.empty) resultingList += reconciledRow

      resultingList ++= leftStrategyRow.tail.map(row => fullRowSchemaWithStatus(row, LHS_DUPLICATE))
      resultingList ++= rightStrategyRow.tail.map(row => fullRowSchemaWithStatus(row, RHS_DUPLICATE))
    }
    resultingList.toIterator
  }


  /* populate seq[Row] with common schema based row for missing row from left and right side */
  private def fullRowSchemaWithStatus(missingRow: Row, sideIdentifier: String): Row = {
    val emptySeq = Seq("", "", "", "")
    sideIdentifier match {
      case MISSING_LEFT | RHS_DUPLICATE => Row.fromSeq(createEmptyRowBasedOnSchema(reconConfigDataHolder.leftDataSetSchema) ++ missingRow.toSeq ++ emptySeq :+ sideIdentifier)
      case MISSING_RIGHT | LHS_DUPLICATE => Row.fromSeq(missingRow.toSeq ++ createEmptyRowBasedOnSchema(reconConfigDataHolder.rightDataSetSchema) ++ emptySeq :+ sideIdentifier)
    }
  }


  /* Create seq with empty values from supplied schema  */
  def createEmptyRowBasedOnSchema(schema: StructType): Seq[Any] = {
    schema.fields.map {
      x =>
        x.dataType.typeName.toLowerCase match {
          case "integer" => 0
          case "double" | "long" => 0.0
          case _ => ""
        }
    }.toSeq
  }


  private def treatDuplicateAsPerStrategy(rightDedupCaseClass: DeduplicateCaseClass, rightSeq: Seq[Row]) = {
    val duplicateMarkingStrategy = rightDedupCaseClass.strategy;
    val duplicateOnField = rightDedupCaseClass.strategyField
    DuplicateStrategy(duplicateMarkingStrategy).sortRowsAsPerStrategy(rightSeq, duplicateOnField)
  }


  def doRowRecon(leftRow: Row, rightRow: Row)(implicit reconConfigDataHolder: ReconConfigDataHolder): Row = {

    val reconColumns: Seq[String] = reconConfigDataHolder.reconcileColumns ++ reconConfigDataHolder.numericColumns
    var recordStatus = new ListBuffer[Boolean]()
    var fieldLevelDiffValues = new ListBuffer[FieldLevelDiff]()

    reconColumns.map {
      reconColumn =>
        val leftColumn = ReconUtil.findColumns(reconColumn)._1
        val rightColumn = ReconUtil.findColumns(reconColumn)._2
        val leftValue = if (leftRow.getAs[Any](leftColumn) == null) "" else leftRow.getAs[Any](leftColumn)
        val rightValue = if (rightRow.getAs[Any](rightColumn) == null) "" else rightRow.getAs[Any](rightColumn)
        val dataType: String = ReconUtil.getDataTypeFromSchema(leftRow.schema, leftColumn).typeName
        val matchStatus: Boolean = dataType.toLowerCase.trim match {
          case "integer" | "float" | "double" | "long" => ReconUtil.numericComparision(leftRow.getAs[Any](leftColumn), rightRow.getAs[Any](rightColumn), dataType)
          case _ => ReconUtil.stringComparision(leftRow.getAs[String](leftColumn), rightRow.getAs[String](rightColumn))
        }
        recordStatus += matchStatus
        if (!matchStatus)
          fieldLevelDiffValues += FieldLevelDiff(leftColumn, rightColumn, leftValue.toString, rightValue.toString)

    }

    val leftReconciliedColList: String = fieldLevelDiffValues.map(x => x.leftcolumn).mkString("|")
    val rightReconciliedColList: String = fieldLevelDiffValues.map(x => x.rightcolumn).mkString("|")
    val leftReconciliedColValueList: String = fieldLevelDiffValues.map(x => x.leftcolumnvalue).mkString("|")
    val rightReconciliedColValueList: String = fieldLevelDiffValues.map(x => x.rightcolumnvalue).mkString("|")


    recordStatus.find(_.equals(false)) match {
      case Some(x) => Row.fromSeq(leftRow.toSeq ++ rightRow.toSeq.:+(leftReconciliedColList).:+(rightReconciliedColList).:+(leftReconciliedColValueList).:+(rightReconciliedColValueList).:+(BREAK))
      case None => Row.fromSeq(leftRow.toSeq ++ rightRow.toSeq.:+(leftReconciliedColList).:+(rightReconciliedColList).:+(leftReconciliedColValueList).:+(rightReconciliedColValueList).:+(MATCH_RECORD))
    }
  }
}

