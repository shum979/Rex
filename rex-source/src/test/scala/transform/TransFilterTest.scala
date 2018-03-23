package transform

import org.apache.spark.sql.Row
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import rex.core.common.{ManagedDataset, SparkContextProvider}
import rex.src.transform.TransConfig.FilterConfig
import rex.src.transform.TransFilter._

class TransFilterTest extends WordSpec with Matchers with BeforeAndAfterAll with SparkContextProvider with Serializable {
  val filePath = this.getClass.getResource("/data/test.txt").getPath

  val readerOptions =
    Map("data.source.csv.file.path" -> filePath,
      "sep" -> ",",
      "data.source.type" -> "csv",
      "header" -> "true",
      "inferSchema" -> "true"
    )


  "filter1" should {
    "filter records based on condition 'age >10'" in {
      val actualResult = {
        val conf = Map("Source_file_name" -> "/data/test.txt")
        val datset: ManagedDataset = new ManagedDataset(spark.read.options(readerOptions).csv(filePath), collection.immutable.Map[String, String]())
        val filteredDataset: ManagedDataset = datset.filterData(FilterConfig("101", "${age} >10", None))
        filteredDataset.take(2)
      }
      val expectedResult = {
        val r1: Row = Row("b", 20, 200, "noida")
        val r2: Row = Row("ca", 30, 400, "gurgaon")
        Array(r1, r2)
      }
      actualResult shouldBe expectedResult
    }
  }


  "filter2" should {
    "filter records based on condition 'age <350 || name == 'c'" in {
      val actualResult = {
        val conf = Map("Source_file_name" -> "/data/test.txt")
        val datset: ManagedDataset = new ManagedDataset(spark.read.options(readerOptions).csv(filePath), collection.immutable.Map[String, String]())
        val filteredDataset: ManagedDataset = datset.filterData(FilterConfig("102", "${age} < 30 || ${name}.substring(0,1) == \"c\"", None))
        filteredDataset.take(3)
      }
      val expectedResult = {
        val r1: Row = Row("b", 20, 200, "noida")
        val r2: Row = Row("ca", 30, 400, "gurgaon")
        val r3: Row = Row("a", 10, 100, "dehradun")
        Array(r3, r1, r2)
      }
      actualResult shouldBe expectedResult
    }
  }

  "filter3" should {
    "filter records based on condition 'age >10' && name =='c'" in {
      val actualResult = {
        val conf = Map("Source_file_name" -> "/data/test.txt")
        val datset: ManagedDataset = new ManagedDataset(spark.read.options(readerOptions).csv(filePath), collection.immutable.Map[String, String]())
        val filteredDataset: ManagedDataset = datset.filterData(FilterConfig("103", "${age} >10 && ${name}.substring(0,1) == \"c\"", None))
        filteredDataset.take(1)
      }
      val expectedResult = {
        val r2: Row = Row("ca", 30, 400, "gurgaon")
        Array(r2)
      }
      actualResult shouldBe expectedResult
    }
  }

  "filter4" should {
    "filter records based on condition 'age >10 && name == 'c' && sal > 200'" in {
      val actualResult = {
        val conf = Map("Source_file_name" -> "/data/test.txt")
        val datset: ManagedDataset = new ManagedDataset(spark.read.options(readerOptions).csv(filePath), collection.immutable.Map[String, String]())
        val filteredDataset: ManagedDataset = datset.filterData(FilterConfig("104", "${age} > 10 && ${name}.substring(0,1) == \"c\" && ${sal} > 200", None))
        filteredDataset.take(1)
      }
      val expectedResult = {
        val r2: Row = Row("ca", 30, 400, "gurgaon")
        Array(r2)
      }
      actualResult shouldBe expectedResult
    }
  }
}