package rex.main.handlers

import rex.core.common.ManagedDataset
import rex.core.xml.{ExportType, ExportTypeOption}
import rex.src.export.{DatasetWriter, ReconExportConfigHelper}

import scala.collection.immutable.Seq
import scalaxb.DataRecord

/**
  * Created by Shubham Gupta on 13-Jan-18.
  * Handler for Export service
  */
object ExportHandler {

  def export(referenceMap: collection.Map[String, ManagedDataset], meta: ExportType) = {
    val specificExportTag = meta.exporttypeoption.get
    val managedDatasetOption = referenceMap.get(meta.applyondataref)

    def exportConfigForRecon = ReconExportConfigHelper.configFrom(referenceMap, meta)

    val seqOfConfigAndDataset = managedDatasetOption.fold(exportConfigForRecon)(md => Seq((specificExportTag, md)))
    seqOfConfigAndDataset.foreach(pair => writeDataWithConfig(pair._1, pair._2))
  }

  def writeDataWithConfig(specificExportTag: DataRecord[ExportTypeOption], managedDataset: ManagedDataset) =
    DatasetWriter(specificExportTag).write(managedDataset)

}
