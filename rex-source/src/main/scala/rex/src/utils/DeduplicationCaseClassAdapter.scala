package rex.src.utils

import rex.core.xml.DeDuplicationType
import rex.src.deduplication.DeduplicateCaseClass
import rex.src.utils.AppConstants._


/**
  * This adapter converts a DeDuplicationType case class to a DeduplicateCaseClass case class
  */
object DeduplicationCaseClassAdapter {

  /**
    * The apply method to convert DeDuplicationType case class to DeduplicateCaseClass case class
    *
    * @param deDuplicationType
    * @return
    */
  def apply(deDuplicationType: DeDuplicationType): DeduplicateCaseClass = {
    val columnPrefix: String = deDuplicationType.side match {
      case Some(value) => if (SOURCE_SIDE == value.toString()) LHS_PRIFIX else if (TARGET_SIDE == value.toString) RHS_PRIFIX else ""
      case None => ""
    }

    val groupingKeys: Seq[String] = deDuplicationType.DeDupColumns.Column.map(columnPrefix + _)
    val strategy: String = deDuplicationType.Strategy match {
      case Some(value) => value.value
      case None => DEFAULT_STRATEGY
    }
    val strategyField: String = deDuplicationType.Strategy match {
      case Some(value) => value.columns match {
        case Some(value) => columnPrefix + value
        case None => ""
      }
      case None => ""
    }
    DeduplicateCaseClass(groupingKeys, strategy, strategyField)
  }
}
