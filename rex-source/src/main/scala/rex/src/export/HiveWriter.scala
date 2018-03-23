package rex.src.export

import rex.core.common.ManagedDataset
import rex.core.xml.HiveTableType

/**
  * Created by Shubham Gupta on 11-Mar-18.
  */
class HiveWriter(exportConfig: HiveTableType) extends DatasetWriter {

  override def write(managedDataset: ManagedDataset): Unit = {

  }
}
