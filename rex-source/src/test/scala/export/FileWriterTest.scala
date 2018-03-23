package export

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import common.TestSparkContextProvider
import org.scalatest.{Matchers, WordSpec}
import rex.core.common.ManagedDataset
import rex.core.xml.ExportType
import rex.src.export.DatasetWriter

import scala.xml.XML

/**
  * Created by Shubham Gupta on 12-Mar-18.
  */
class FileWriterTest extends WordSpec with Matchers with TestSparkContextProvider with Serializable {

  val projectPath = System.getProperty("user.dir")

  "FileWriterTest" should {
    "save dataset in parquet format" in {
      val exportXml =
        raw"""<Export name="inputDataset" applyondataref="ReconFlow">
           |    <File format="parquet">
           |        <TargetStore>local</TargetStore>
           |        <FileLocation>$projectPath/target/parquetTest</FileLocation>
           |        <Mode>Overwrite</Mode>
           |    </File>
           |</Export>"""

      val exportType = scalaxb.fromXML[ExportType](XML.loadString(exportXml))
      val sellerDataset: ManagedDataset = managedDatasetFromFile("/data/reporting_data.txt")

      DatasetWriter(exportType.exporttypeoption.get).write(sellerDataset)
      val output = spark.read.parquet(s"$projectPath/target/parquetTest")

      output.count() shouldBe 5
      output.first().mkString(",") shouldBe "9001,Institutional,High,25-Jul-2014,1.3,2017-03-29 16:02:25,124"
    }

    "save dataset in json format" in {
      val exportXml =
        raw"""<Export name="inputDataset" applyondataref="ReconFlow">
           |    <File format="json">
           |        <TargetStore>local</TargetStore>
           |        <FileLocation>$projectPath/target/jsonTest</FileLocation>
           |        <Mode>Overwrite</Mode>
           |    </File>
           |</Export>"""

      val exportType = scalaxb.fromXML[ExportType](XML.loadString(exportXml))
      val sellerDataset: ManagedDataset = managedDatasetFromFile("/data/reporting_data.txt")

      DatasetWriter(exportType.exporttypeoption.get).write(sellerDataset)
      val output = spark.read.json(s"$projectPath/target/jsonTest")

      output.count() shouldBe 5
      output.first().mkString(",") shouldBe "2017-03-29 16:02:25,High,Institutional,25-Jul-2014,124,9001,1.3"
    }


    "save dataset in csv format with properPartition" in {
      val exportXml =
        raw"""<Export name="inputDataset" applyondataref="ReconFlow">
           |    <File format="csv">
           |        <TargetStore>local</TargetStore>
           |        <FileLocation>$projectPath/target/partitionTest</FileLocation>
           |         <Partition>
           |              <DatePartitionFormat>$$DAY</DatePartitionFormat>
           |              <ColumnPartition>
           |                 <Column>SellerImpact</Column>
           |              </ColumnPartition>
           |         </Partition>
           |        <Mode>Overwrite</Mode>
           |    </File>
           |</Export>"""

      val exportType = scalaxb.fromXML[ExportType](XML.loadString(exportXml))
      val sellerDataset: ManagedDataset = managedDatasetFromFile("/data/reporting_data.txt")

      DatasetWriter(exportType.exporttypeoption.get).write(sellerDataset)
      val day = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))

      spark.read.csv(s"$projectPath/target/partitionTest/$day/SellerImpact=High")
      spark.read.csv(s"$projectPath/target/partitionTest/$day/SellerImpact=Impactful")
      spark.read.csv(s"$projectPath/target/partitionTest/$day/SellerImpact=Low")
      spark.read.csv(s"$projectPath/target/partitionTest/$day/SellerImpact=VeryHigh")
    }
  }

}
