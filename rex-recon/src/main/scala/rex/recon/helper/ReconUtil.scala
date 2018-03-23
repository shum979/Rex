package rex.recon.helper

import org.apache.spark.sql.types.{StructType, _}
import rex.core.xml.ReconciliationType
import rex.src.deduplication.DeduplicateCaseClass
import rex.src.utils.AppConstants._
import rex.src.utils.DeduplicationCaseClassAdapter

/**
  * Created by Vishal on 1/20/2018.
  */
object ReconUtil {


  def getFieldLevelBreakSchema(): StructType = {
    StructType(
      StructField(LEFTCOLUMNNAME, StringType, false) ::
        StructField(RIGHTCOLUMNNAME, StringType, false) ::
        StructField(LEFTCOLUMNVALUE, StringType, false) ::
        StructField(RIGHTCOLUMNVALUE, StringType, false) ::
        Nil
    )
  }

  def getDataTypeFromSchema(schema: StructType, fieldName: String): DataType = {
    schema.fields.find(x => x.name.equalsIgnoreCase(fieldName.trim)) match {
      case Some(value) => value.dataType
      case None => StringType
    }
  }

  /*
  Get data type of given field from supplied schema
   */

  /*
  used for numeric comparision between two values based on datatype
   */
  def numericComparision(value1: Any, value2: Any, dataType: String): Boolean = {
    dataType match {
      case INT => compareValue[Int](value1, value2)
      case LONG => compareValue[Long](value1, value2)
      case DOUBLE => compareValue[Double](value1, value2)
      case FLOAT => compareValue[Float](value1, value2)
      case _ => value1 == value2
    }
  }

  def compareValue[A](value1: Any, value2: Any): Boolean = {
    try {
      value1.asInstanceOf[A] == value2.asInstanceOf[A]
    } catch {
      case e: Exception => false
    }
  }

  def stringComparision(value1: String, value2: String) = {
    value1 == value2
  }

  def createReconConfigDataHolder(reconConfig: ReconciliationType): ReconConfigDataHolder = {

    //Add prefic as 'LHS_' in all left side and RHS_ in all right side columns(comparision key column, reconcilie columns, numericcolumns etc) to avoid ambiguity
    val numericColumns: Seq[String] = reconConfig.NumericColumns match {
      case Some(value) => value.Column.map { x => val leftRightColumns = ReconUtil.findColumns(x); s"$LHS_PRIFIX${leftRightColumns._1}=$RHS_PRIFIX${leftRightColumns._2}" }
      case None => Nil
    }

    val reconcileColumns: Seq[String] = reconConfig.ReconcileColumns match {
      case Some(value) => value.Column.map { x => val leftRightColumns = ReconUtil.findColumns(x); s"$LHS_PRIFIX${leftRightColumns._1}=$RHS_PRIFIX${leftRightColumns._2}" }
      case None => Nil
    }

    val comparisionKeyColumns: Seq[String] = reconConfig.ComparisonKey.Column.map { x => val leftRightColumns = ReconUtil.findColumns(x); s"$LHS_PRIFIX${leftRightColumns._1}=$RHS_PRIFIX${leftRightColumns._2}" }


    val getLeftMatchingKey: Seq[String] = comparisionKeyColumns.map(x => ReconUtil.findColumns(x)._1)
    val getRightMatchingKey: Seq[String] = comparisionKeyColumns.map(x => ReconUtil.findColumns(x)._2)

    //Convert SbxDeduplicationType to Deduplication case class
    val leftDedupCaseClass: DeduplicateCaseClass = DeduplicationCaseClassAdapter(reconConfig.DeDuplicationStrategy.filter(sbxDedupType => SOURCE_SIDE.eq(sbxDedupType.side.getOrElse("").toString)).head)
    val rightDedupCaseClass: DeduplicateCaseClass = DeduplicationCaseClassAdapter(reconConfig.DeDuplicationStrategy.filter(sbxDedupType => TARGET_SIDE.eq(sbxDedupType.side.getOrElse("").toString)).head)

    val leftDataSetSchema: StructType = StructType(StructField("", StringType, false) :: Nil)
    val rightDataSetSchema: StructType = StructType(StructField("", StringType, false) :: Nil)

    ReconConfigDataHolder(numericColumns,
      reconcileColumns,
      comparisionKeyColumns,
      getLeftMatchingKey,
      getRightMatchingKey,
      leftDedupCaseClass,
      rightDedupCaseClass,
      leftDataSetSchema,
      rightDataSetSchema
    )
  }

  /*
  populate ReconConfigDataHolder case class from supplied reconConfig of xml
  ReconConfigDataHolder is used to supply recon config related details at different layers
   */

  def findColumns(str: String): (String, String) = {
    try {
      (str.split("=")(0).trim, str.split("=")(1).trim)
    }
    catch {
      case ex: Exception => throw new AssertionError(s"$str is not valid key structure for left and right side columns")
    }
  }

  /*
  Creating combined schema used to bound cogroup return row
  this would be merging od left and right plus field level difference from both side in case of break records
   */

  def createCombinedSchemaAfterMerging(leftSchema: StructType, rightSchema: StructType): StructType = {
    StructType(leftSchema.union(rightSchema))
      .add(StructField(LEFTCOLUMNNAME, StringType, false))
      .add(StructField(RIGHTCOLUMNNAME, StringType, false))
      .add(StructField(LEFTCOLUMNVALUE, StringType, false))
      .add(StructField(RIGHTCOLUMNVALUE, StringType, false))
      .add(StructField(ROW_STATUS, StringType, false))
  }


}
