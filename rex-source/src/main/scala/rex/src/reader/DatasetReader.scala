package rex.src.reader

import rex.core.common.{CommonConstants, ManagedDataset}
import rex.core.xml.{DataSourceTypeOption, FileType}

import scalaxb.DataRecord


/**
  * Created by Shubham Gupta on 12/6/2017.
  * this is reader contract -- Any developer wishing to add additional
  * datasource needs to implement this contract.
  */
trait DatasetReader {

  final def readData() = {
    validateManifest()
    getData()
  }

  /*final user is not aware about this method as it is called internally
  but every datasource should provide how to validate it's manifest*/
  protected def validateManifest(): Boolean

  /*method for constructing dataset from configurations*/
  protected def getData(): ManagedDataset
}


/**
  * this is factory class for getting appropriate datasource reader
  * based on key element coming in xml manifest
  */
object DatasetReader {

  def apply(sourceManifest: DataRecord[DataSourceTypeOption]): DatasetReader = sourceManifest.key.get match {
    case CommonConstants.DATASOURCE_FILE_TAG => FileReader(sourceManifest.as[FileType])
    case unknown => throw new IllegalArgumentException(s"Currently $unknown format is not supported")
  }


}