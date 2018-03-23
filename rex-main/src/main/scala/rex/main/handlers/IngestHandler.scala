package rex.main.handlers

import rex.core.common.ManagedDataset
import rex.core.xml.{DataSourceType, IngestType}
import rex.src.reader.DatasetReader

import scala.collection.mutable

/**
  * Created by Shubham Gupta on 24-Jan-18.
  * handler for ingesting datasource
  */
object IngestHandler {

  // accepting ingest configuration and binding it with actual class
  def ingest(ingestFlow: IngestType, dataSources: Seq[DataSourceType], referenceMap: mutable.Map[String, ManagedDataset]) = {
    val datasourceNm = ingestFlow.applyondataref
    val source: DataSourceType = dataSources.filter(x => x.name == datasourceNm)(0)

    val sourceReader = DatasetReader(source.datasourcetypeoption)
    val data = sourceReader.readData()
    referenceMap += (ingestFlow.name -> data)
  }

}
