package rex.src.transform

import rex.core.common.ManagedDataset
import rex.core.xml.TransformationType

/**
  * Created by Shubham Gupta on 24-Feb-18.
  */
object SimpleDuplicateRemover extends Transformer {

  /**
    * Implemented method from implemented trait. It performs deduplication task
    *
    * @param datasetMap
    * @param config
    * @return ManagedDataset
    */
  override def transform(datasetMap: Map[String, ManagedDataset], config: TransformationType): ManagedDataset = {
    // Calling get directly might lead to exception. But this shall take place only if transform type is provided wrongly
    // i.e. Transformation type is chosen DeDuplicationTransformation and xml tag for some other transformation
    // are provided
    val meta = config.DeDuplication.get
    val managedDataset = datasetMap(config.applyondataref)
    managedDataset.dropDuplicates(meta.DeDupColumns.Column.toArray)
  }
}
