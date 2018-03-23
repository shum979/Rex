package rex.core.common

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType

/**
  * Created by Shubham Gupta on 12/7/2017.
  */

/**
  * this is central abstraction in entire accelerator/recon project
  * this is used to intercept all dataset operation so that
  * additional capability of exception handling, auditing can be plugged easily
  *
  * @param dataframe
  * @param metadata
  */
case class ManagedDataset(private val dataframe: Dataset[Row], metadata: Map[String, String]) {

  /** count method to simply count dataset rows */
  def count = dataframe.count()

  /** wrapper method for dataset take */
  def take(n: Int) = dataframe.take(n)

  /** wrapper method for removing duplicate records,It is plain vanilla deduplicate, it removes duplicate records */
  def dropDuplicates(columns: Array[String]) = ManagedDataset(dataframe.dropDuplicates(columns), metadata)

  /** wrapper method to write data on disc in specified format  */
  def save(outputPath: String, format: String, mode: String, configMap: Map[String, String] = Map.empty)(partitionColumns: Option[Seq[String]] = None) = {
    val writer = dataframe.write.mode(SaveMode.valueOf(mode)).options(configMap).format(format)

    if (partitionColumns.isDefined) {
      writer.partitionBy(partitionColumns.get: _*).save(outputPath)
    } else {
      writer.save(outputPath)
    }
  }

  /** wrapper method for registering dataset as temp view */
  def register(name: String) = dataframe.createTempView(name)

  /** wrapper method for join  -- Column names as spark-column */
  def join(right: ManagedDataset, usingColumns: Column, joinType: String): ManagedDataset = {
    ManagedDataset(this.dataframe.join(right.dataframe, usingColumns, joinType), metadata)
  }

  import org.apache.spark.sql.functions._

  /** wrapper method for join -- column name as plain string */
  def join(right: ManagedDataset, usingColumns: String, joinType: String): ManagedDataset = {
    ManagedDataset(this.dataframe.join(right.dataframe, expr(usingColumns), joinType), metadata)
  }

  /** wrapper method to return columns of dataset */
  def columns(): Array[String] = {
    dataframe.columns
  }

  /** wrapper method for selecting columns */
  def selectExpr(exprs: String*): ManagedDataset = {
    ManagedDataset(dataframe.selectExpr(exprs: _*), metadata)
  }

  /** wrapper method for selecting columns */
  def select(cols: Column*): ManagedDataset = {
    ManagedDataset(dataframe.select(cols: _*), metadata)
  }

  /** wrapper method for show method of dataset */
  def show(): Unit = {
    dataframe.show(false)
  }

  /** wrapper method for filter method of dataset */
  def filter(condition: Column): ManagedDataset = {
    ManagedDataset(dataframe.filter(condition), metadata)
  }


  /** wrapper method for schema method of dataset */
  def schema(): StructType = {
    dataframe.schema
  }

  /** wrapper method for filter method of dataset on Row level */
  def filter(func: Row => Boolean): ManagedDataset = {
    ManagedDataset(dataframe.filter(func), metadata)
  }

  /** wrapper method for map method of dataset */
  def map(func: Row => Row, encoder: ExpressionEncoder[Row]): ManagedDataset = {
    ManagedDataset(dataframe.map(row => func(row))(encoder), metadata)
  }

  /** wrapper method for coalesce */
  def coalesce(numberOfPartitions: Int): ManagedDataset = {
    ManagedDataset(dataframe.coalesce(numberOfPartitions), metadata)
  }

  /** wrapper method to get getNumPartitions */
  def getPartition: Int = {
    dataframe.rdd.getNumPartitions
  }

  /** wrapper method of the mapPartitions method of datatset */
  def mapPartitions(mapPartitionFunc: Iterator[Row] => Iterator[Row], encoder: ExpressionEncoder[Row]): ManagedDataset = {
    ManagedDataset(dataframe.mapPartitions(mapPartitionFunc)(encoder), metadata)
  }

  /** wrapper method for flatmap method of dataset */
  def flatMap(func: Row => Seq[Row], encoder: ExpressionEncoder[Row]): ManagedDataset = {
    ManagedDataset(dataframe.flatMap(row => func(row))(encoder), metadata)
  }

  /** wrapper method for persist method of dataset */
  def persist() = {
    dataframe.persist()
  }

  /** wrapper method for withColumn method of dataset */
  def withColumn(columnName: String, col: Column) = {
    ManagedDataset(dataframe.withColumn(columnName, col), metadata)
  }

  /** method for getting job id of current spark app */
  def getSparkJobID(): String = {
    dataframe.sparkSession.sparkContext.applicationId
  }

  /** wrapper method for dropping columns from dataset --takes single spark-column */
  def drop(col: Column) = {
    ManagedDataset(dataframe.drop(col), metadata)
  }

  /** wrapper method for dropping columns from dataset -- takes multiple column name as string  */
  def drop(colNames: String*) = {
    ManagedDataset(dataframe.drop(colNames: _*), metadata)
  }

  /** wrapper method to group the dataset by key */
  def groupByKey(groupingFunc: (Row) => (String))(encoder: Encoder[String]): KeyValueGroupedDataset[String, Row] = {
    dataframe.groupByKey(groupingFunc)(encoder)
  }

  /** wrapper method for where method of the dataset */
  def where(exprs: String): ManagedDataset = {
    ManagedDataset(dataframe.where(exprs), metadata)
  }

  /** get comma separated list of all files with constitute this dataset */
  def getFileNames(): String = {
    dataframe.inputFiles.mkString(",")
  }

  /** wrapper for printing schema */
  def printSchema() = dataframe.printSchema()

  /** creating temp view -- it's scope is strongly tied to sparkSession.
    * this view is present as long as spark session object is alive */
  def createTempView(viewName: String) = {
    dataframe.createTempView(viewName)
  }

  /** wrapper method for except in */
  def except(filteredData: ManagedDataset): ManagedDataset = {
    ManagedDataset(dataframe.except(filteredData.dataframe), metadata)
  }

  /** wrapper method for repartition method in datatset */
  def repartition(partitions: Int): ManagedDataset = {
    ManagedDataset(dataframe.repartition(partitions), metadata)
  }

  /** wrapper method to collect dataset in an Array */
  def collect(): Array[Row] = {
    dataframe.collect()
  }

  /** wrapper method to get column from dataset based on column name. */
  def apply(colName: String): Column = {
    dataframe.apply(colName)
  }

  /** wrapper method to get spark session from dataset */
  def getSparkSession: SparkSession = {
    dataframe.sparkSession
  }

  /**
    * wrapper method for group by operation on managedDataset
    *
    * @param aggregation is Map of columns and aggregate functions as depicted in example
    * @param columns
    * @return ManagedDataset
    *
    *         example :
    * ds.groupBy($"department", $"gender").agg(Map("salary" -> "avg","age" -> "max"))
    */
  def groupBy(aggregation: Map[String, String], columns: Seq[Column]) = {
    val result = dataframe.groupBy(columns: _*)
    ManagedDataset(result.agg(aggregation), metadata)
  }

  /* wrapper method to group the dataset by columns*/
  def groupBy(cols: List[Column]): RelationalGroupedDataset = {
    dataframe.groupBy(cols: _*)
  }

  /*wrapper method for re-naming column of dataset */
  def withColumnRenamed(existingName: String, newName: String) = {
    ManagedDataset(dataframe.withColumnRenamed(existingName, newName), metadata)
  }
}

