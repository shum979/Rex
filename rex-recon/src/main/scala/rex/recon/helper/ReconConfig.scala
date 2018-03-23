package rex.recon.helper

import org.apache.spark.sql.types.StructType
import rex.src.deduplication.DeduplicateCaseClass

/**
  * Created by Shubham A Gupta on 22-Mar-18.
  */

case class FieldLevelDiff(leftcolumn: String, rightcolumn: String, leftcolumnvalue: String, rightcolumnvalue: String)

case class ReconConfigDataHolder(numericColumns: Seq[String],
                                 reconcileColumns: Seq[String],
                                 comparisionKeyColumns: Seq[String],
                                 getLeftMatchingKey: Seq[String],
                                 getRightMatchingKey: Seq[String],
                                 leftDedupCaseClass: DeduplicateCaseClass,
                                 rightDedupCaseClass: DeduplicateCaseClass,
                                 leftDataSetSchema: StructType,
                                 rightDataSetSchema: StructType
                                )

