package rex.src.reader

import rex.core.common.ManagedDataset
import rex.core.xml.FileType

/**
  * Created by Shubham Gupta on 13-Mar-18.
  */
class ParquetFileReader(private val fileType: FileType) extends FileReader(fileType) {

  override protected def validateManifest(): Boolean = true;

  override protected def readFromFile(): ManagedDataset = {
    val dataset = spark.read.parquet(filePath)
    ManagedDataset(dataset, rowMetadata)
  }
}
