package deduplication

import common.TestSparkContextProvider
import org.scalatest.{Matchers, WordSpec}
import rex.core.common.ManagedDataset
import rex.core.xml.DeDuplicationType
import rex.src.deduplication.Deduplicator
import rex.src.utils.AppConstants._
import rex.src.utils.DeduplicationCaseClassAdapter

import scala.xml.XML

/**
  * Created by Shubham A Gupta on 1/31/2018.
  */
class DeduplicateTest extends WordSpec with Matchers with TestSparkContextProvider {

  spark.sparkContext.setLogLevel("ERROR")

  "Deduplicator" should {
    "filter duplicate records in given dataset for MAX Strategy" in {
      val filePath = this.getClass.getResource("/data/sampleDuplicateFile.txt").getPath

      val fileDs = spark.sqlContext.read.option("header", "true").csv(filePath)
      val xmlDeDuplicateMAX = """<DeDuplication>
                                |	<DeDupColumns>
                                |		<Column>name</Column>
                                |		<Column>risk</Column>
                                |	</DeDupColumns>
                                |	<Strategy columns = "amount">MAX</Strategy>
                                |</DeDuplication>
                                |""" stripMargin

      val deDuplicationType = scalaxb.fromXML[DeDuplicationType](XML.loadString(xmlDeDuplicateMAX))

      val managedDs = ManagedDataset(fileDs, Map("" -> ""))
      val duplicateConfig = DeduplicationCaseClassAdapter(deDuplicationType)
      val result = new Deduplicator(duplicateConfig).deduplicate(managedDs)

      val filteredData = result.filter(result.apply(IS_DUPLICATE_COLUMN) === IS_DUPLICATE_VALUE_NOT_DUPLICATE)
      assert(filteredData.count === 4)
    }

    "filter duplicate records in given dataset for MIN Strategy" in {
      val filePath = this.getClass.getResource("/data/sampleDuplicateFile.txt").getPath

      val fileDs = spark.sqlContext.read.option("header", "true").csv(filePath)
      val xmlDeDuplicateMIN = """<DeDuplication>
                                |	<DeDupColumns>
                                |		<Column>name</Column>
                                |		<Column>risk</Column>
                                |	</DeDupColumns>
                                |	<Strategy columns = "amount">MIN</Strategy>
                                |</DeDuplication>
                                |""" stripMargin

      val deDuplicationType = scalaxb.fromXML[DeDuplicationType](XML.loadString(xmlDeDuplicateMIN))
      val managedDs = ManagedDataset(fileDs, Map("" -> ""))

      val duplicateConfig = DeduplicationCaseClassAdapter(deDuplicationType)
      val result = new Deduplicator(duplicateConfig).deduplicate(managedDs)

      val filteredData = result.filter(result.apply(IS_DUPLICATE_COLUMN) === IS_DUPLICATE_VALUE_NOT_DUPLICATE)
      assert(filteredData.count === 4)
    }

    "filter duplicate records in given dataset for DROP All Duplicate Strategy" in {
      val filePath = this.getClass.getResource("/data/sampleDuplicateFile.txt").getPath

      val fileDs = spark.sqlContext.read.option("header", "true").csv(filePath)
      val xmlDeDuplicateDrop = """<DeDuplication>
                                 |	<DeDupColumns>
                                 |		<Column>name</Column>
                                 |		<Column>risk</Column>
                                 |	</DeDupColumns>
                                 |	<Strategy columns = "amount">DROP</Strategy>
                                 |</DeDuplication>
                                 |""" stripMargin

      val deDuplicationType = scalaxb.fromXML[DeDuplicationType](XML.loadString(xmlDeDuplicateDrop))
      val managedDs = ManagedDataset(fileDs, Map("" -> ""))

      val duplicateConfig = DeduplicationCaseClassAdapter(deDuplicationType)
      val result = new Deduplicator(duplicateConfig).deduplicate(managedDs)

      val filteredData = result.filter(result.apply(IS_DUPLICATE_COLUMN) === IS_DUPLICATE_VALUE_NOT_DUPLICATE)
      assert(filteredData.count === 2)
    }

    "filter duplicate records in given dataset for Default Duplicate Strategy" in {
      val filePath = this.getClass.getResource("/data/sampleDuplicateFile.txt").getPath

      val fileDs = spark.sqlContext.read.option("header", "true").csv(filePath)
      val xmlDeDuplicateDefault =
        """
          |<DeDuplication>
          |	<DeDupColumns>
          |		<Column>name</Column>
          |		<Column>risk</Column>
          |	</DeDupColumns>
          |</DeDuplication>
        """.stripMargin

      val deDuplicationType = scalaxb.fromXML[DeDuplicationType](XML.loadString(xmlDeDuplicateDefault))

      val managedDs = ManagedDataset(fileDs, Map("" -> ""))

      val duplicateConfig = DeduplicationCaseClassAdapter(deDuplicationType)
      val result = new Deduplicator(duplicateConfig).deduplicate(managedDs)

      val filteredData = result.filter(result.apply(IS_DUPLICATE_COLUMN) === IS_DUPLICATE_VALUE_NOT_DUPLICATE)
      assert(filteredData.count === 4)
    }

    "filter duplicate records in given dataset for NO Action Duplicate Strategy" in {
      val filePath = this.getClass.getResource("/data/sampleDuplicateFile.txt").getPath

      val fileDs = spark.sqlContext.read.option("header", "true").csv(filePath)
      val xmlDeDuplicateNOACT =
        """
          |<DeDuplication>
          |	<DeDupColumns>
          |		<Column>name</Column>
          |		<Column>risk</Column>
          |	</DeDupColumns>
          |	<Strategy columns = "amount">NO_ACTION </Strategy>
          |</DeDuplication>
        """.stripMargin

      val duplicationType = scalaxb.fromXML[DeDuplicationType](XML.loadString(xmlDeDuplicateNOACT))
      val managedDs = ManagedDataset(fileDs, Map("" -> ""))
      val duplicateConfig = DeduplicationCaseClassAdapter(duplicationType)
      val result = new Deduplicator(duplicateConfig).deduplicate(managedDs)
      assert(result.count === 10)
    }

    "filter duplicate records in given dataset for invalid column name" in {
      val filePath = this.getClass.getResource("/data/sampleDuplicateFile.txt").getPath

      val fileDs = spark.sqlContext.read.option("header", "true").csv(filePath)
      val xmlDeDuplicateInvalid =
        """
          |<DeDuplication>
          |	<DeDupColumns>
          |		<Column>name</Column>
          |		<Column>risks</Column>
          |	</DeDupColumns>
          |	<Strategy columns = "amount">MAX</Strategy>
          |</DeDuplication>
        """.stripMargin

      val deDuplicationType = scalaxb.fromXML[DeDuplicationType](XML.loadString(xmlDeDuplicateInvalid))
      val managedDs = ManagedDataset(fileDs, Map("" -> ""))

      val thrown = intercept[IllegalArgumentException] {
        val duplicateConfig = DeduplicationCaseClassAdapter(deDuplicationType)
        val result = new Deduplicator(duplicateConfig).deduplicate(managedDs)
      }
      assert(thrown.getMessage === "Cannot resolve column name \"risks\" among (name, age, risk, amount, date)")
    }
  }
}
