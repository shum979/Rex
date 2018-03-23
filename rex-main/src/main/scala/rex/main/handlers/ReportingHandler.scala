package rex.main.handlers

import rex.core.common.ManagedDataset
import rex.core.xml.ReportingType
import rex.src.reporting.Reporter

import scala.collection.{Map, mutable}

/**
  * Created by Shubham Gupta on 14-Feb-18.
  */
object ReportingHandler {

  /*this is a delegate method for processing of reporting tag*/
  def report(reportingManifest: ReportingType, referenceMap: mutable.Map[String, ManagedDataset]) = {
    val tables: Array[String] = reportingManifest.applyondataref.split(";")

    // extracting only dataset which are required by reporting operation
    val mds: Map[String, ManagedDataset] = referenceMap.filterKeys(tables.toSet)
    Reporter.report(mds, reportingManifest)
  }
}
