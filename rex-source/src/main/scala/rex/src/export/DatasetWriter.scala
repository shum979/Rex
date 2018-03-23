package rex.src.export

import rex.core.common.CommonConstants._
import rex.core.common.ManagedDataset
import rex.core.xml.{ExportFileType, ExportTypeOption, HiveTableType}

import scalaxb.DataRecord

/**
  * Created by Shubham Gupta on 11-Mar-18.
  */
trait DatasetWriter {
  def write(managedDataset: ManagedDataset)
}


object DatasetWriter {

  def apply(exportConfig: DataRecord[ExportTypeOption]) = {
    val xmlTag = exportConfig.key.get

    xmlTag match {
      case FILE_EXPORT_TAG => new FileWriter(exportConfig.as[ExportFileType])
      case HIVE_EXPORT_TAG => new HiveWriter(exportConfig.as[HiveTableType])
      case unknown => throw new UnsupportedOperationException(s"Export tag $unknown is not supported yet")
    }
  }


}