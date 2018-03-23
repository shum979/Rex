package rex.recon.implement

import java.util.Calendar

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, Row}
import rex.core.common.{ManagedDataset, RexConfig}
import rex.core.xml.ReconciliationType
import rex.recon.helper.ReconUtil
import rex.recon.helper.ReconcileUDF._
import rex.recon.writer.{DataStoreTryHelper, InvalidDataStoreHandlerFactory}
import rex.src.deduplication.Deduplicator
import rex.src.utils.AppConstants._
import rex.src.utils.DeduplicationCaseClassAdapter

import scala.collection.mutable.ListBuffer


/**
  * Created by Amit on 12/5/2017.
  */
class ReconWithJoin(sbxReconConfig: ReconciliationType) extends Serializable with RexConfig with DataStoreTryHelper {

  //Add prefic as 'LHS_' in all left side and RHS_ in all right side columns(comparision key column, reconcilie columns, numericcolumns etc) to avoid ambiguity
  val numericColumns = sbxReconConfig.NumericColumns match {
    case Some(value) => value.Column.map { x => val leftRightColumns = ReconUtil.findColumns(x); s"$LHS_PRIFIX${leftRightColumns._1}=$RHS_PRIFIX${leftRightColumns._2}" }
    case None => Nil
  }
  val reconcileColumns = sbxReconConfig.ReconcileColumns match {
    case Some(value) => value.Column.map { x => val leftRightColumns = ReconUtil.findColumns(x); s"$LHS_PRIFIX${leftRightColumns._1}=$RHS_PRIFIX${leftRightColumns._2}" }
    case None => Nil
  }
  val comparisionKeyColumns = sbxReconConfig.ComparisonKey.Column.map { x => val leftRightColumns = ReconUtil.findColumns(x); s"$LHS_PRIFIX${leftRightColumns._1}=$RHS_PRIFIX${leftRightColumns._2}" }
  val columnsLeft: Seq[String] = (comparisionKeyColumns ++ numericColumns ++ reconcileColumns).map(column => ReconUtil.findColumns(column)._1)
  val columnsRight: Seq[String] = (comparisionKeyColumns ++ numericColumns ++ reconcileColumns).map(column => ReconUtil.findColumns(column)._2)
  val getMatchingKeyString = comparisionKeyColumns.map(x => x.replaceFirst("=", "|")).mkString(",")
  val getLeftMatchingKey: Seq[String] = comparisionKeyColumns.map(x => ReconUtil.findColumns(x)._1)
  val getRightMatchingKey: Seq[String] = comparisionKeyColumns.map(x => ReconUtil.findColumns(x)._2)
  //Convert SbxDeduplicationType to Deduplication case class
  val leftDedupCaseClass = DeduplicationCaseClassAdapter(sbxReconConfig.DeDuplicationStrategy.filter(sbxDedupType => SOURCE_SIDE.eq(sbxDedupType.side.getOrElse("").toString)).head)
  val rightDedupCaseClass = DeduplicationCaseClassAdapter(sbxReconConfig.DeDuplicationStrategy.filter(sbxDedupType => TARGET_SIDE.eq(sbxDedupType.side.getOrElse("").toString)).head)

  def findMissing(joinedRow: Row, columns: Array[(String)]): Boolean = {
    if (joinedRow.getAs[Any](columns.head) != null)
      false
    else
      findMissing(joinedRow, columns.tail)
  }

  def reconcile(leftDataSet: ManagedDataset, rightDataSet: ManagedDataset): (ManagedDataset, ManagedDataset) = {

    //Add LHS_ as prefix in all columns of left dataset to avoid ambiguity
    val renamedColumns = leftDataSet.columns.map(c => col(c).as(s"$LHS_PRIFIX$c"))
    val renamedColumnLeftDataSet: ManagedDataset = leftDataSet.select(renamedColumns: _*)

    //Add RHS_ as prefix in all columns of right dataset to avoid ambiguity
    val rightRenamedColumn = rightDataSet.columns.map(c => col(c).as(s"$RHS_PRIFIX$c"))
    val renamedColumnRightDataSet: ManagedDataset = rightDataSet.select(rightRenamedColumn: _*)

    val sparkApplicationID = leftDataSet.getSparkJobID()
    val currentdate: Column = current_date()

    // Filter Duplicates on the Source and Target Datasets according to the Strategy mentioned in the XML with the Strategy Field
    val leftDeDuplicatedDataset = new Deduplicator(leftDedupCaseClass).deduplicate(renamedColumnLeftDataSet)
    val rightDeDuplicatedDataset = new Deduplicator(rightDedupCaseClass).deduplicate(renamedColumnRightDataSet)

    //Create Row Encoders for the Source and Target datasets
    val duplicateRowEncoderLeft: ExpressionEncoder[Row] = RowEncoder.apply(leftDeDuplicatedDataset.schema())
    val duplicateRowEncoderRight: ExpressionEncoder[Row] = RowEncoder.apply(rightDeDuplicatedDataset.schema())

    def dummyFunc(row: Row): Row = {
      row
    }

    //create matching key combination for joining left and right dataset with AND operation
    val matchingColumnExpression: String = comparisionKeyColumns.map {
      value =>
        val splitedColumns = ReconUtil.findColumns(value)
        val left = splitedColumns._1
        val right = splitedColumns._2
        s"$left <=> $right"
    }.mkString(" AND ")


    val applicationID = leftDeDuplicatedDataset.getSparkSession.sparkContext.applicationId

    //Write the duplicates to a file using mapPartitions method
    val leftDatsetCoalesced = leftDeDuplicatedDataset.coalesce(50).mapPartitions(mapPartitionFunc(getDuplicateRecordsPathLocation(applicationID), dummyFunc), duplicateRowEncoderLeft)
    val rightDatsetCoalesced = rightDeDuplicatedDataset.coalesce(50).mapPartitions(mapPartitionFunc(getDuplicateRecordsPathLocation(applicationID), dummyFunc), duplicateRowEncoderRight)

    //Joined two datasets based on comparison key
    val joinedData: ManagedDataset = leftDatsetCoalesced.join(rightDatsetCoalesced, expr(matchingColumnExpression), "fullouter")

    //Apply record level status on each row of joined dataset. Ex: MISSING_LEFT, MISSING_RIGHT, RECONCILIED
    val allcols = missingLeftOrRight(columnsLeft.toArray, columnsRight.toArray).alias(RECONRDSTATUS) :: (columnsLeft.map(col(_)).toList ++ columnsRight.map(col(_)).toList)
    val fullJoinedDataSet = joinedData.select(allcols: _*).withColumn(RECON_DATE, currentdate).withColumn(JOBID, lit(sparkApplicationID))


    //Break RECONCILIED record status into BREAK or MATCH status. final rowstatus will have: MISSING_LEFT, MISSING_RIGHT, BREAK, MATCH
    val joinedEncoder: ExpressionEncoder[Row] = RowEncoder(StructType(StructType(StructField(ROW_STATUS, StringType, false) :: Nil) ++ fullJoinedDataSet.schema()))
    val recordLevelReconciliedDataSet = fullJoinedDataSet.map(findRowStatus, joinedEncoder).drop(col(RECONRDSTATUS)).withColumn(MATCHINGKEY, lit(getMatchingKeyString))


    //Create new field level dataset for all BREAK records : mathcing keys from left and right dataset, left column, right column, left value, right value
    val leftMatchingStructField: Seq[StructField] = renamedColumnLeftDataSet.schema().filter(structField => getLeftMatchingKey.exists(x => x.equalsIgnoreCase(structField.name)))
    val rightMatchingStructField: Seq[StructField] = renamedColumnRightDataSet.schema().filter(structField => getRightMatchingKey.exists(x => x.equalsIgnoreCase(structField.name)))

    val fieldLevelRecordEncoder: ExpressionEncoder[Row] = RowEncoder(StructType(leftMatchingStructField ++ rightMatchingStructField ++ ReconUtil.getFieldLevelBreakSchema))
    val fieldLevelReconciledDataSet = recordLevelReconciliedDataSet.filter(expr(s"$ROW_STATUS =='$BREAK'"))
      .flatMap(findFieldLevelDiffBasedOnReconciledKey, fieldLevelRecordEncoder).withColumn(RECON_DATE, currentdate).withColumn(JOBID, lit(sparkApplicationID))

    (recordLevelReconciliedDataSet, fieldLevelReconciledDataSet)
  }

  /**
    * This method returns the location of the files to write the duplicate records
    *
    * @param applicationID
    * @return
    */
  def getDuplicateRecordsPathLocation(applicationID: String): String = {
    val location = config.getString("tempdatalocation.duplicate")
    val cal = Calendar.getInstance()
    val date = cal.get(Calendar.DATE)
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH) + 1
    s"${location}/${year}/${month}/${date}/${applicationID}/duplicate/"
  }

  /**
    * This method goes into the mapPartition function which takes location of the files to write and the processing function as arguments and writes the files
    *
    * @param locationPath
    * @param func
    * @return
    */
  def mapPartitionFunc(locationPath: String, func: Row => Row) = {
    (row: Iterator[Row]) =>
      val context = TaskContext.get
      val stageId = context.stageId()
      val partionId = context.partitionId()

      val defaultHandler = InvalidDataStoreHandlerFactory.getFileHandler(locationPath, stageId + partionId + "dprecords")
      withDataStoreHelperOnRowIterator(row, defaultHandler) {
        func
      }
  }


  /*
  Method to accept row and return row by adding new column as 'rowstatus' in each row
  rowstatus can be: MISSING_LEFT, MISSING_RIGHT, BREAK, MATCH
   */
  def findRowStatus(row: Row): Row = {

    var recordStatus = new ListBuffer[Boolean]()
    val numericAndReconciledColumns: Seq[String] = reconcileColumns ++ numericColumns
    val returnedRow: Row = row.getAs[String](RECONRDSTATUS) match {
      case MISSING_LEFT | MISSING_RIGHT => val seqRow = row.toSeq.+:(row.getAs[String](RECONRDSTATUS))
        Row(seqRow: _*)
      case RECONCILIED => numericAndReconciledColumns.map { x => recordStatus += findDiffInFieldFromRow(row, x) }
        val breakStatus = recordStatus.find(_.equals(false)) match {
          case Some(x) => BREAK
          case None => MATCH_RECORD
        }
        val seqRow = row.toSeq.+:(breakStatus)
        Row(seqRow: _*)
    }
    returnedRow
  }

  /*
  Method accept row and reconciliedColumns as string(ex: lhs_name=name)
  It return status of match for given reconcilied column in that row
  true : if columns match else false
   */

  def findFieldLevelDiffBasedOnReconciledKey(row: Row): Seq[Row] = {

    val numericAndReconciledColumns: Seq[String] = reconcileColumns ++ numericColumns

    val matchedRow = numericAndReconciledColumns.map { x =>
      val splittedColumns = ReconUtil.findColumns(x)
      val leftColumn = splittedColumns._1
      val rightColumn = splittedColumns._2
      val missmatch = findDiffInFieldFromRow(row, x)
      val leftValue = if (row.getAs[Any](leftColumn) == null) "" else row.getAs[Any](leftColumn)
      val rightValue = if (row.getAs[Any](rightColumn) == null) "" else row.getAs[Any](rightColumn)
      (leftColumn, rightColumn, leftValue.toString, rightValue.toString, missmatch)
    }

    val leftMatchingKey: Seq[String] = getLeftMatchingKey.map { x => val left = row.getAs[Any](x); if (left == null) "" else left.toString }
    val rightMatchingKey: Seq[String] = getRightMatchingKey.map { x => val right = row.getAs[Any](x); if (right == null) "" else right.toString }

    matchedRow.filter(x => x._5.equals(false)).map { x =>
      val finalSeq: Seq[Any] = leftMatchingKey ++ rightMatchingKey ++ (x._1 :: x._2 :: x._3 :: x._4 :: Nil)
      Row(finalSeq: _*)
    }
  }


  /*
  Method to find field level breaks in given row
  accept row and return Seq[Row] for all fields level break
  Return row schema= all matching keys, leftsidecolumnname, rightsidecolumnname, leftcolumnvalue, rightcolumnvalue
   */

  def findDiffInFieldFromRow(row: Row, reconciledColumns: String): Boolean = {
    val splitedColumns = ReconUtil.findColumns(reconciledColumns)
    val leftColumn = splitedColumns._1
    val rightColumn = splitedColumns._2
    val dataType: String = ReconUtil.getDataTypeFromSchema(row.schema, leftColumn).typeName.toLowerCase
    val missmatch: Boolean = dataType.trim match {
      case INT | FLOAT | DOUBLE | LONG => ReconUtil.numericComparision(row.getAs[Any](leftColumn), row.getAs[Any](rightColumn), dataType)
      case _ => ReconUtil.stringComparision(row.getAs[String](leftColumn), row.getAs[String](rightColumn))
    }
    missmatch
  }


  /*//Sample WIP function for single recon table with all details, still approach to be decide and function to be refactor
    def compareFieldsBasedOnReconKey(row :Row): Row ={

      val additionalRequiredAttributes=new scala.collection.mutable.HashMap[String,String]()
      val leftSideReconciledColumns=new scala.collection.mutable.HashMap[String,String]()
      val rightSideReconciledColumns=new scala.collection.mutable.HashMap[String,String]()
      var recordStatus = new ListBuffer[Boolean]()

      comparisionKeyColumns.map{x =>
        val left=x.split("=")(0)
        val right=x.split("=")(1)
        additionalRequiredAttributes +=(s"$left|$right" -> row.getAs[String](left))
      }

      val numericAndReconciledColumns: Seq[String] =reconcileColumns ++ numericColumns

      row.getAs[String](RECONRDSTATUS) match {
        case RECONCILIED =>  numericAndReconciledColumns.map{ x =>
                        val leftColumn=x.split("=")(0)
                        val rightColumn=x.split("=")(1)
                        val dataType: String =ReconUtil.getDataTypeFromSchema(row.schema,leftColumn).typeName
                        val matchStatus: Boolean =dataType.toLowerCase.trim match{
                          case INT | FLOAT | DOUBLE | LONG => ReconUtil.numericComparision(row.getAs[Any](leftColumn),row.getAs[Any](rightColumn),dataType)
                          case _ => ReconUtil.stringComparision(row.getAs[String](leftColumn),row.getAs[String](rightColumn))
                        }
                        val leftValue=if(row.getAs[String](leftColumn)==null) "" else row.getAs[String](leftColumn)
                        val rightValue=if(row.getAs[String](rightColumn)==null) "" else row.getAs[String](rightColumn)

                        leftSideReconciledColumns +=(s"$leftColumn" -> leftValue)
                        rightSideReconciledColumns +=(s"$rightColumn" -> rightValue)
                        recordStatus +=matchStatus
                      }
        case _ => None

      }
      val breakStatus=recordStatus.find(_.equals(false)) match {
        case Some(x) => BREAK
        case None => if(row.getAs[String](RECONRDSTATUS).equals(RECONCILIED)) MATCH_RECORD else row.getAs[String](RECONRDSTATUS)
      }
      val rows: Seq[Any] =row.toSeq.+:(additionalRequiredAttributes).+:(rightSideReconciledColumns).+:(leftSideReconciledColumns).+:(breakStatus.toString)
      Row(rows:_*)
    }


    def compareFieldsBasedOnReconKeyForSingleTable(row :Row): Seq[Row] ={

      val additionalRequiredAttributes=new scala.collection.mutable.HashMap[String,String]()
      val leftSideReconciledColumns=new scala.collection.mutable.HashMap[String,String]()
      val rightSideReconciledColumns=new scala.collection.mutable.HashMap[String,String]()
      val matchKeyMap=new scala.collection.mutable.HashMap[String,String]()
      var recordStatus = new ListBuffer[Boolean]()
      var fieldLevelDiffValues = new ListBuffer[FieldLevelDiff]()

      comparisionKeyColumns.map{x =>
        val left=x.split("=")(0)
        val right=x.split("=")(1)
        matchKeyMap +=(s"$left|$right" -> row.getAs[String](left))
      }

      val numericAndReconciledColumns: Seq[String] =reconcileColumns ++ numericColumns
      val rowStatus=row.getAs[String](RECONRDSTATUS)

      numericAndReconciledColumns.map{x =>
        val leftColumn=x.split("=")(0)
        val rightColumn=x.split("=")(1)
        val leftValue=if(row.getAs[String](leftColumn)==null) "" else row.getAs[String](leftColumn)
        val rightValue=if(row.getAs[String](rightColumn)==null) "" else row.getAs[String](rightColumn)

        leftSideReconciledColumns +=(s"$leftColumn" -> leftValue)
        rightSideReconciledColumns +=(s"$rightColumn" -> rightValue)

        rowStatus match {
          case RECONCILIED => val dataType: String =ReconUtil.getDataTypeFromSchema(row.schema,leftColumn).typeName
            val matchStatus: Boolean =dataType.toLowerCase.trim match{
              case INT | FLOAT | DOUBLE | LONG => ReconUtil.numericComparision(row.getAs[Any](leftColumn),row.getAs[Any](rightColumn),dataType)
              case _ => ReconUtil.stringComparision(row.getAs[String](leftColumn),row.getAs[String](rightColumn))
            }
            recordStatus +=matchStatus
            if(!matchStatus)
              fieldLevelDiffValues +=FieldLevelDiff(leftColumn,rightColumn,leftValue,rightValue)
          case _ => Nil

        }
      }

      recordStatus.find(_.equals(false)) match {
        case Some(x) => fieldLevelDiffValues.map{(x: FieldLevelDiff) =>
          RowFactory.create(matchKeyMap,leftSideReconciledColumns, rightSideReconciledColumns,matchKeyMap,x.leftcolumn,x.rightcolumn,x.leftcolumnvalue,x.rightcolumnvalue,BREAK ) }
        case None => val finalStatus= if(rowStatus.equals(RECONCILIED)) MATCH_RECORD else rowStatus
          Seq(RowFactory.create(matchKeyMap,leftSideReconciledColumns, rightSideReconciledColumns,matchKeyMap,"","","","", finalStatus ))
      }
    }*/


}

//case class FieldLevelDiff(leftcolumn:String,rightcolumn:String,leftcolumnvalue:String,rightcolumnvalue:String)
object ReconWithJoin {
  def apply(reconConfig: ReconciliationType): ReconWithJoin = new ReconWithJoin(reconConfig)
}

