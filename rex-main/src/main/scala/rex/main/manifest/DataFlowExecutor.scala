package rex.main.manifest

import rex.core.common.ManagedDataset
import rex.core.xml.{DataFlowType, DataSourceType, Export, ExportType, Ingest, IngestType, Reconcile, ReconciliationType, Reporting, ReportingType, Transform, TransformationsType}
import rex.main.handlers._

import scala.collection.mutable

/**
  * Created by Shubham Gupta on 11-Jan-18.
  * this is entry point for XML execution
  * it takes xml file path as argument,
  * creates concrete pipeline from it and execute it finally
  */
object DataFlowExecutor {

  val referenceMap = mutable.Map[String, ManagedDataset]()

  def execute(fileName: String): Unit = {
    val dataFlows = XmlParser.parseXmlFromFile(fileName)

    UserPropertyHandler.parseProperties(dataFlows.Properties)

    val dataSources = dataFlows.DataSources.get.DataSource
    dataFlows.DataFlow.foreach(flow => runFlow(flow, dataSources))
  }


  /**
    * main method doing all binding of xml tags to processing class implementations
    * developer note : add a case to add new flow feature
    *
    * @param dataflow
    * @param dataSources
    */
  private def runFlow(dataflow: DataFlowType, dataSources: Seq[DataSourceType]) = {
    val flow = dataflow.Flow

    flow.foreach(flowType => {
      flowType.typeValue match {
        case Ingest => ingest(flowType.Ingest, dataSources)
        case Transform => transform(flowType.Transformations)
        case Export => store(flowType.Export)
        case Reconcile => reconcile(flowType.Reconciliation)
        case Reporting => report(flowType.Reporting)
        case other => throw new UnsupportedOperationException("unknown flow operation :" + other.toString)
      }
    })
  }


  /*delegating ingest operation to it's handler*/
  def ingest(operation: Option[IngestType], dataSources: Seq[DataSourceType]) = {
    operation.map(ingestManifest => IngestHandler.ingest(ingestManifest, dataSources, referenceMap))
  }


  /*delegating transform operation to it's handler*/
  def transform(operation: Option[TransformationsType]) = {
    operation.map(transformManifest => TransformHandler.transform(referenceMap, transformManifest))
  }

  /* delegating store operation to it's handler*/
  def store(operation: Option[ExportType]) = {
    operation.map(exportManifest => ExportHandler.export(referenceMap, exportManifest))
  }

  /* delegating reconcile operation to it's handler*/
  def reconcile(operation: Option[ReconciliationType]) = {
    operation.map(reconManifest => ReconHandler.recon(referenceMap, reconManifest))
  }


  /* delegating reporting operation to it's handler*/
  def report(operation: Option[ReportingType]) = {
    operation.map(reportingManifest => ReportingHandler.report(reportingManifest, referenceMap))
  }


}
