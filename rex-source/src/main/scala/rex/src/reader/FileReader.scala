package rex.src.reader

import java.util.Calendar

import rex.core.common.CommonConstants._
import rex.core.common.{ManagedDataset, SparkContextProvider}
import rex.core.helper.CommonUtils
import rex.core.xml.{CsvValue, FileType, ParquetValue}

import scala.collection.mutable

/** Created by Shubham Gupta on 12/6/2017.
  * this class takes configuration from manifest service and reads data into specified format
  */
private[reader] abstract class FileReader(val metadata: FileType) extends DatasetReader with SparkContextProvider with Serializable {

  val readerConfigMap = mutable.Map[String, String]()
  val filePath: String = resolveFilePath(metadata.DirectoryPath, metadata.FileNameOrPattern)

  /*properties specified by <Tags> tag in xml*/
  val tagMap: Map[String, String] = metadata.Tags.map(CommonUtils.buildTagToMap).getOrElse(Map.empty[String, String])

  val rowMetadata = {
    Map(INPUT_FILE_NAME -> filePath,
      USER_NAME -> CommonUtils.getApplicationUser,
      PROCESSING_TIMESTAMP -> Calendar.getInstance.getTime.toString
    ) ++ tagMap
  }


  final override def getData(): ManagedDataset = {
    val basicDataset = readFromFile()
    DatasourceConfigurator.applyConfiguration(basicDataset, metadata)
  }

  /** This helper method checks if file name of input file is given, if it is given it appends
    * name after directory location otherwise it simply return directory path */
  def resolveFilePath(directoryPath: String, filePathOption: Option[String]): String = {
    filePathOption.fold(directoryPath)(directoryPath + FILE_SEPARATOR + _)
  }

  protected def readFromFile(): ManagedDataset
}

object FileReader {
  def apply(fileType: FileType): FileReader = {

    fileType.format match {
      case CsvValue => new CsvFileReader(fileType)
      case ParquetValue => new ParquetFileReader(fileType)
    }


  }
}