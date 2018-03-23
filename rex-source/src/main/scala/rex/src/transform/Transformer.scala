package rex.src.transform

import rex.core.common.ManagedDataset
import rex.core.xml.{ColumnTransformation, DeDuplicationTransformation, ExpressionTransformation, GroupByTransformation, JoinTransformation, TransCategory, TransformationType}

/**
  * Created by Shubham Gupta on 24-Feb-18.
  * This is base trait for any transformation. Every transformation should mixin this trait
  */
trait Transformer {

  /**
    * worker method for implementing functionality of transformation action
    *
    * @param datasetMap
    * @param config
    * @return
    */
  def transform(datasetMap: Map[String, ManagedDataset], config: TransformationType): ManagedDataset

}

/**
  * this is factory class for getting specific implementation of any transformation
  */
object Transformer {
  def apply(transformationType: TransCategory): Transformer = transformationType match {
    case DeDuplicationTransformation => SimpleDuplicateRemover
    case ExpressionTransformation => ExpressionTransformer
    case GroupByTransformation => GroupByTransformer
    case JoinTransformation => JoinTransformer
    case ColumnTransformation => ColumnTransformer
    case other => throw new UnsupportedOperationException(s"operation $other is not supported as of now")
  }
}

