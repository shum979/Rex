package rex.src.export

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import rex.core.common.ManagedDataset
import rex.core.xml.ExportFileType
import rex.src.utils.AppConstants._

/**
  * Created by Shubham Gupta on 11-Mar-18.
  */
class FileWriter(fileType: ExportFileType) extends DatasetWriter {


  /*store managed dataset into csv file -- at present locally*/
  override def write(managedDataset: ManagedDataset) = {
    val mode = fileType.Mode.getOrElse("ErrorIfExists")
    val format = fileType.format.toString
    val storePath = fileType.FileLocation

    //------------------------Extra configurations-------------------------
    val header = fileType.IncludeHeader.getOrElse(false)
    val separator = fileType.Delimiter.getOrElse(",")

    val configMap = Map("header" -> header.toString, "sep" -> separator)

    //------------------------partition capabilities--------------------
    val resolvedPath = fileType.Partition.map { partition =>
      val dateFormat = partition.DatePartitionFormat.get
      dateFormat match {
        case PARTITION_DAY_IDENTIFIER => LocalDate.now().format(DateTimeFormatter.ofPattern(DEFAULT_DATE_FORMAT))
        case _ => LocalDateTime.now().format(DateTimeFormatter.ofPattern(dateFormat))
      }
    }.fold(storePath)(partitionPath => storePath + "/" + partitionPath)

    val partitionColumns = fileType.Partition.flatMap { partition =>
      partition.ColumnPartition.map { columns => columns.Column }
    }
    //-------------------------------------------------------------------
    managedDataset.save(resolvedPath, format, mode, configMap)(partitionColumns)
  }


}
