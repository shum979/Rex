package rex.main.handlers

import org.apache.spark.sql.SaveMode
import rex.core.common.{ManagedDataset, RexConfig}
import rex.core.xml.{ExportType, FileType, ReconciliationType}
import rex.recon.Reconciler
import rex.recon.helper.ReconPostProcessingReports
import rex.src.reader.FileReader
import rex.src.utils.AppConstants._

import scala.collection.mutable
import scala.xml.XML

/**
  * Created by Shubham Gupta on 25-Jan-18.
  */
object ReconHandler extends RexConfig {

  // accepting recon configuration and passing it rightly to Reconciler
  def recon(referenceMap: mutable.Map[String, ManagedDataset], reconFlow: ReconciliationType) = {

    doRecon(referenceMap, reconFlow)
    doSave(referenceMap, reconFlow)
    doReconPostProcessing(referenceMap)
    doSavePostProcessingReports(referenceMap)
  }

  def doRecon(referenceMap: mutable.Map[String, ManagedDataset], reconFlow: ReconciliationType): referenceMap.type = {
    val leftDS: ManagedDataset = referenceMap(reconFlow.Source)
    val rightDS: ManagedDataset = referenceMap(reconFlow.Target)

    val result = Reconciler(reconFlow).recon(leftDS, rightDS)
    referenceMap += (reconFlow.name -> result)
  }

  def doSave(referenceMap: mutable.Map[String, ManagedDataset], reconFlow: ReconciliationType) = {

    val fileLocation = config.getString("recon.tablelocation") + "/" + RECON_RESULT
    val mode = SaveMode.Overwrite.toString

    val exportConfig = csvExportConfigs(fileLocation, mode).getConfig
    ExportHandler.writeDataWithConfig(exportConfig, referenceMap(reconFlow.name))
  }

  def doReconPostProcessing(referenceMap: mutable.Map[String, ManagedDataset]) = {

    val directoryPath = config.getString("recon.tablelocation") + "/" + RECON_RESULT
    val fileXml = raw"""<File format="csv"><DirectoryPath>$directoryPath</DirectoryPath><Header>true</Header></File>"""
    val file = scalaxb.fromXML[FileType](XML.loadString(fileXml))

    val reader = FileReader(file)
    val dataset: ManagedDataset = reader.readData
    val postProcessedMap = ReconPostProcessingReports.createBreakRecordsDataSet(dataset)
    postProcessedMap.foreach { case (key, manageddataSet) => referenceMap += (key -> manageddataSet)
    }
  }

  def doSavePostProcessingReports(referenceMap: mutable.Map[String, ManagedDataset]) = {
    val breakExportConfig = csvExportConfigs(config.getString("recon.tablelocation") + "/" + BREAK_RESULT, SaveMode.Overwrite.toString).getConfig
    ExportHandler.writeDataWithConfig(breakExportConfig, referenceMap(BREAK_RESULT))

    val summaryExportConfig = csvExportConfigs(config.getString("recon.tablelocation") + "/" + SUMMARY_RESULT, SaveMode.Overwrite.toString).getConfig
    ExportHandler.writeDataWithConfig(summaryExportConfig, referenceMap(SUMMARY_RESULT))

  }

  case class csvExportConfigs(fileLocation: String, saveMode: String) {
    def getConfig = {
      val exportXml = raw"""<Export name="inputDataset" applyondataref="ReconFlow"><File format="csv"><TargetStore>local</TargetStore><FileLocation>$fileLocation</FileLocation><Mode>$saveMode</Mode><IncludeHeader>true</IncludeHeader></File></Export>"""
      scalaxb.fromXML[ExportType](XML.loadString(exportXml)).exporttypeoption.get
    }
  }

}
