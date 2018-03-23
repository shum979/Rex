package rex.src.transform

import org.apache.spark.sql.functions
import rex.core.common.{ManagedDataset, SparkUtils}
import rex.core.xml.{AddColumnType, DeleteColumnType, FromProperties, FromStandardEnvVariable, Literal, RenameType, TransformationType}


/**
  * Created by Shubham Gupta on 27-Feb-18.
  */
object ColumnTransformer extends Transformer {

  /**
    * worker method for implementing functionality of column transformation action
    * it can perperom 3 column coperations
    * 1. add new column
    * 2. rename existing column
    * 3. drop existing column
    *
    * @param datasetMap
    * @param config
    * @return
    */
  override def transform(datasetMap: Map[String, ManagedDataset], config: TransformationType): ManagedDataset = {
    var managedDataset = datasetMap(config.applyondataref)
    val columnTransformationConfig = config.ColumnTransformations.get

    // adding new column only if any add config exists
    columnTransformationConfig.AddNew.map { newColumnsList =>
      newColumnsList.Column.foreach(column => managedDataset = managedDataset ++ column)
    }

    // dropping existing columns only if any drop config exists
    columnTransformationConfig.Delete.map { toBedeletedColums =>
      managedDataset = managedDataset -- toBedeletedColums
    }

    // renaming columns only if any rename config exists
    columnTransformationConfig.Rename.map { renamedColumnList =>
      renamedColumnList.Column.foreach(column => managedDataset = managedDataset rename column)
    }

    managedDataset
  }


  /**
    * this is wrapper class to make managedDataset have capabilities of adding/deleting/renaming columns
    *
    * @param managedDataset
    */
  implicit class managedDatasetWrapper(managedDataset: ManagedDataset) {

    /*this method adds new column to class scoped managedDataset instance*/
    def ++(newColumnConfig: AddColumnType) = {
      val identifier = newColumnConfig.value
      val valueFrom = newColumnConfig.valueFrom
      val nameWithoutDollar = if (identifier.charAt(0) == '$') identifier.substring(1, identifier.length) else identifier

      val resolvedColumnValue = valueFrom match {
        case Literal => nameWithoutDollar
        case FromProperties => SparkUtils.getPropertyValue(nameWithoutDollar).get
        case FromStandardEnvVariable => managedDataset.metadata(nameWithoutDollar)
        case unknown => throw new UnsupportedOperationException(s"column value population not supported by option $unknown")
      }
      managedDataset.withColumn(newColumnConfig.name, functions.lit(resolvedColumnValue))
    }

    /*this method drops existing column of class-scoped managedDataset instance*/
    def --(deletedColumnNames: DeleteColumnType) = {
      managedDataset.drop(deletedColumnNames.Column: _*)
    }

    /*this method renames column-name of class-scoped managedDataset instance*/
    def rename(columnToBeRenamed: RenameType) = {
      managedDataset.withColumnRenamed(columnToBeRenamed.existingName, columnToBeRenamed.newName)
    }
  }

}
