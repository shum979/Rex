package rex.src.export

import rex.core.common.CommonConstants._
import rex.core.common.ManagedDataset
import rex.core.xml.{ExportFileType, ExportType, ExportTypeOption}

import scala.collection.immutable.Seq
import scalaxb.DataRecord

/**
  * Created by Shubham Gupta on 12-Mar-18.
  */
object ReconExportConfigHelper {

  val getReconList = List(RECON_RECORD_LEVEL_RESULT, RECON_FIELD_LEVEL_RESULT)

  def configFrom(referenceMap: collection.Map[String, ManagedDataset], meta: ExportType): Seq[(DataRecord[ExportTypeOption], ManagedDataset)] = {

    val specificExportTag = meta.exporttypeoption.get
    val originalFileLocation = specificExportTag.value.asInstanceOf[ExportFileType].FileLocation

    getReconList.map { appender =>
      val originalConfig = specificExportTag.value.asInstanceOf[ExportFileType]
        .copy(FileLocation = originalFileLocation + FILE_SEPARATOR + appender.substring(1))

      val dataRecord = DataRecord(specificExportTag.namespace, specificExportTag.key, originalConfig)
      val managedDataset = referenceMap(meta.applyondataref + appender)
      (dataRecord, managedDataset)
    }
  }

}
