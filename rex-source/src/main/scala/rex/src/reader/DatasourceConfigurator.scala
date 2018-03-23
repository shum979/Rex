package rex.src.reader

import org.apache.spark.sql.functions._
import rex.core.common.CommonConstants._
import rex.core.common.ManagedDataset
import rex.core.xml.FileType

/**
  * Created by Shubham Gupta on 03-Mar-18.
  */
object DatasourceConfigurator {

  val FILE_NAME_IDENTIFIER = PROPERTY_IDENTIFIER + FILE_NAME

  def applyConfiguration(managedDataset: ManagedDataset, config: FileType): ManagedDataset = {
    val propertyMap: Map[String, String] = managedDataset.metadata

    val columnsWithValues = config.WithColumn.map(newColumnConfig => (newColumnConfig.name, resolve(propertyMap, newColumnConfig.value)))

    columnsWithValues.foldLeft(managedDataset)((mDataset, column) => mDataset.withColumn(column._1, column._2))
  }


  def resolve(propertyMap: => Map[String, String], columnValue: String) = {
    columnValue match {
      case FILE_NAME_IDENTIFIER => input_file_name()
      case property if property.startsWith("$") => lit(propertyMap(property.substring(1)))
      case literalValue => lit(literalValue)
    }
  }


}
