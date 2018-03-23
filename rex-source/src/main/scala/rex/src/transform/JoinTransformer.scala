package rex.src.transform

import rex.core.common.ManagedDataset
import rex.core.xml.TransformationType

/**
  * Created by Shubham Gupta on 24-Feb-18.
  */
object JoinTransformer extends Transformer {


  /**
    * this method implements join functionality -- currently it is limited for 2 datasets and one join condition
    *
    * @param datasetMap -- contains the list of managedDataset needed for this operation
    * @param config     -- this is configuration for Join transformation
    * @return ManagedDataset
    */
  override def transform(datasetMap: Map[String, ManagedDataset], config: TransformationType): ManagedDataset = {
    val managedDataset = datasetMap(config.applyondataref)
    val joinConfig = config.JoinTransformation.get

    val rightDs = datasetMap(joinConfig.RightSideJoiningData)
    val joinKey = joinConfig.JoinKey
    val joinExpr = managedDataset.apply(joinKey.leftSideKey).equalTo(rightDs.apply(joinKey.rightSideKey))
    val colSeq = joinConfig.Select
    val result = managedDataset.join(rightDs, joinExpr, joinConfig.joinType.toString)

    joinConfig.Select match {
      case Some(list) => result.selectExpr(list.Column: _*)
      case None => result
    }

  }
}
