package rex.src.reporting

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{Dataset, Row, SaveMode}
import rex.core.common.{ManagedDataset, SparkContextProvider}
import rex.core.xml.{ReportingSql, ReportingType}

/**
  * Created by Shubham Gupta on 14-Feb-18.
  */
object Reporter extends SparkContextProvider {

  val DATE_TIME_FORMAT = "yyy'_'MMM'_'dd'_'hhmm"


  /*main method which generates reports based on given sqls*/
  def report(inputMap: collection.Map[String, ManagedDataset], reporting: ReportingType) = {
    inputMap.foreach(entry => entry._2.createTempView(entry._1))

    val publishPath: String = atPath(reporting)
    reporting.ReportingSql.foreach(executeQuery(_, publishPath))
  }

  // method which actually executes the sql query against tables
  def executeQuery(sqlObj: ReportingSql, publishPath: String) = {
    val reportingResult = spark.sql(sqlObj.value)
    reportingResult.createTempView(sqlObj.name)
    val finalPath = publishPath + File.separator + sqlObj.name
    localWriter(reportingResult, finalPath)
  }

  // utility method for saving data on Local/Hdfs filesystem
  def localWriter(result: Dataset[Row], path: String) = {
    // TODO : this is just for testing -- remove number of partitions
    val part1 = result.repartition(1)
    part1.write.mode(SaveMode.Overwrite).csv(path)
  }

  // utility method for generating path based on configurations
  def atPath: ReportingType => String = reporting => {
    val time = LocalDateTime.now.format(DateTimeFormatter.ofPattern(DATE_TIME_FORMAT))
    List(reporting.PublishingPath, reporting.name, time).mkString(File.separator)
  }

}
