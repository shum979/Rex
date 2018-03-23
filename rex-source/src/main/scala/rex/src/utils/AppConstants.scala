package rex.src.utils

object AppConstants {

  val DEFAULT_DATE_FORMAT = "yyyyMMdd"
  val PARTITION_DAY_IDENTIFIER = "$DAY"

  /*-------Dedupe constants --------------------*/
  val IS_DUPLICATE_COLUMN = "isduplicate"
  val DUPLICATE_COUNT_COLUMN = "duplicateCount"

  val MAX_STRATEGY = "MAX"
  val MIN_STRATEGY = "MIN"
  val DROP_STRATEGY = "DROP"
  val DEFAULT_STRATEGY = "DEFAULT"
  val NO_ACTION_STRATEGY = "NO_ACTION"

  val IS_DUPLICATE_VALUE_NOT_DUPLICATE = "Not Duplicate"
  val IS_DUPLICATE_VALUE_DUPLICATE = "Duplicate"

  val SOURCE_SIDE = "source"
  val TARGET_SIDE = "target"

  /*-------Recon constants --------------------*/
  val RECONRDSTATUS = "RecordStatus"
  val RECONCILIED = "RECONCILIED"
  val RECON_DATE = "recondate"
  val JOBID = "jobid"
  val BREAK = "BREAK"
  val MATCH_RECORD = "MATCH"
  val MISSING_LEFT = "MISSING_ON_LEFT"
  val MISSING_RIGHT = "MISSING_ON_RIGHT"
  val LHS_DUPLICATE = "DUPLICATE_ON_LEFT"
  val RHS_DUPLICATE = "DUPLICATE_ON_RIGHT"
  val prefix = "lhs_"
  val LHS_PRIFIX = "lhs_"
  val RHS_PRIFIX = "rhs_"
  val ROW_STATUS = "rowstatus"

  val INT = "integer"
  val LONG = "long"
  val FLOAT = "float"
  val DOUBLE = "double"

  val MATCHINGKEY = "matchingKey"
  val LEFTCOLUMNNAME = "leftcolumnname"
  val RIGHTCOLUMNNAME = "rightcolumnname"
  val LEFTCOLUMNVALUE = "leftcolumnvalue"
  val RIGHTCOLUMNVALUE = "rightcolumnvalue"
  val MATCHSTATUS = "matchstatus"

  val RECON_RESULT = "recon_result"
  val SUMMARY_RESULT = "summary_result"
  val BREAK_RESULT = "break_result"

}
