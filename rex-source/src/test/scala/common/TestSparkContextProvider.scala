package common

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rex.core.common.ManagedDataset

/**
  * Created by Shubham Gupta on 12/6/2017.
  */
trait TestSparkContextProvider {

  lazy val spark = SparkSession
    .builder()
    .appName("Rex-ReconWithAccelerator")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("spark.default.parallelism", "1")

  val readerOptions = Map("sep" -> "|", "data.source.type" -> "csv", "header" -> "true")

  def managedDatasetFromFile(filePathRelativeToProject: String) = {
    new ManagedDataset(dataFrameReader(filePathRelativeToProject), Map[String, String]().empty)
  }

  def dataFrameReader(filePathRelativeToProject: String, configMap: Map[String, String] = readerOptions): Dataset[Row] = {
    val absFilePath = this.getClass.getResource(filePathRelativeToProject).getPath
    spark.read.options(configMap).csv(absFilePath)
  }

}

