package rex.src.reader

import org.apache.spark.sql.types.StructType
import rex.core.common.ManagedDataset
import rex.core.helper.SchemaBuilder
import rex.core.xml.FileType

/**
  * Created by Shubham Gupta on 13-Mar-18.
  */
class CsvFileReader(private val fileType: FileType) extends FileReader(fileType) {

  // TODO : implement validateManifest method
  /* this method validates for correctness of all incoming parameters*/
  override def validateManifest(): Boolean = true;

  /* this is internal method for actually loading data via spark reader*/
  override def readFromFile(): ManagedDataset = {
    var dataFrameReader = spark.read
    if (!metadata.Header.getOrElse(false)) {
      withSchema match {
        case Some(schema) => dataFrameReader = dataFrameReader.schema(schema)
        case None => throw new IllegalStateException("No schema is provided for datasource " + filePath)
      }
    }
    ManagedDataset(dataFrameReader.options(withCsvConfigs).csv(filePath), rowMetadata)
  }


  /*translate incoming parameters into spark csv reader API constants*/
  private def withCsvConfigs = {
    readerConfigMap += ("sep" -> metadata.Delimiter.getOrElse(","),
      "header" -> metadata.Header.getOrElse(false).toString
    )
  }

  /**
    * this method parses schema -- first priority is given to <schema> tag of XML
    * if <schema> tag is not available then it looks for <schemaPath> tag - which is a path of file
    *
    * @return Option[StructType]
    */
  private def withSchema(): Option[StructType] = {
    metadata.Schema.map { schemaType =>
      schemaType.enforceMode.map(mode => readerConfigMap += ("mode" -> mode.toString.toUpperCase))
      if (!schemaType.SchemaColumn(0).dataType.isDefined) readerConfigMap += ("inferSchema" -> "true")
      SchemaBuilder.buildFromXmlColumnList(schemaType.SchemaColumn)
    } orElse {
      metadata.SchemaFilePath.map(SchemaBuilder.buildFromJsonFile)
    }
  }
}


