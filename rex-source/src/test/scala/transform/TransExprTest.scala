package transform

import org.apache.spark.sql.Row
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import rex.core.common.{ManagedDataset, SparkContextProvider}
import rex.core.xml.{ExpressionTransformation, TransformationsType}
import rex.src.transform.ExpressionTransformer

import scala.xml.XML

class TransExprTest extends WordSpec with Matchers with BeforeAndAfterAll with SparkContextProvider with Serializable {
  val filePath = this.getClass.getResource("/data/test.txt").getPath

  val readerOptions =
    Map("data.source.csv.file.path" -> filePath,
      "sep" -> ",",
      "data.source.type" -> "csv",
      "header" -> "true",
      "inferSchema" -> "true"
    )


  "expr1" should {
    "Should evaluate expressions in expression_test_1.xml" in {
      val actualResult = {
        val xml_file = this.getClass.getResource("/xmls/expression_test_1.xml").getPath
        val transformations = scalaxb.fromXML[TransformationsType](XML.loadFile(xml_file))
        val indataset: ManagedDataset = new ManagedDataset(spark.read.options(readerOptions).csv(filePath), collection.immutable.Map[String, String]())
        var exprDataset: Array[Row] = new Array[Row](0)

        transformations.Transformation.foreach(transformationType => {
          transformationType.transCategory match {
            case ExpressionTransformation => {
              exprDataset = ExpressionTransformer.transform(Map(transformationType.applyondataref -> indataset), transformationType).take(3)
            }
            case other => new IllegalArgumentException(s"operation $other is not supported as of now")
          }

        })
        exprDataset

      }

      val expectedResult = {
        val r1: Row = Row("a", 10, 600, "dehradun", 1000, 300)
        val r2: Row = Row("b", 20, 700, "noida", 4000, 400)
        val r3: Row = Row("ca", 30, 500, "gurgaon", 12000, 500)
        Array(r1, r2, r3)
      }
      actualResult shouldBe expectedResult
    }

  }
  "expr2" should {
    "Should evaluate expressions in expression_test_2.xml" in {
      val actualResult = {
        val xml_file = this.getClass.getResource("/xmls/expression_test_2.xml").getPath
        val transformations = scalaxb.fromXML[TransformationsType](XML.loadFile(xml_file))
        val indataset: ManagedDataset = new ManagedDataset(spark.read.options(readerOptions).csv(filePath), collection.immutable.Map[String, String]())
        var exprDataset: Array[Row] = new Array[Row](0)

        transformations.Transformation.foreach(transformationType => {
          transformationType.transCategory match {
            case ExpressionTransformation => {
              exprDataset = ExpressionTransformer.transform(Map(transformationType.applyondataref -> indataset), transformationType).take(3)
            }
            case other => new IllegalArgumentException(s"operation $other is not supported as of now")
          }

        })
        exprDataset
      }

      val expectedResult = {
        val r1: Row = Row("a", 10, 150, "dehradun", "GOOD EMPLOYEE")
        val r2: Row = Row("b", 20, 250, "noida", "BAD EMPLOYEE")
        val r3: Row = Row("ca", 30, 420, "gurgaon", "BAD EMPLOYEE")
        Array(r1, r2, r3)
      }
      actualResult shouldBe expectedResult
    }

  }
  "expr3" should {
    "Should evaluate expressions in expression_test_3.xml" in {
      val actualResult = {
        val xml_file = this.getClass.getResource("/xmls/expression_test_3.xml").getPath
        val transformations = scalaxb.fromXML[TransformationsType](XML.loadFile(xml_file))
        val indataset: ManagedDataset = new ManagedDataset(spark.read.options(readerOptions).csv(filePath), collection.immutable.Map[String, String]())
        var exprDataset: Array[Row] = new Array[Row](0)

        transformations.Transformation.foreach(transformationType => {
          transformationType.transCategory match {
            case ExpressionTransformation => {
              exprDataset = ExpressionTransformer.transform(Map(transformationType.applyondataref -> indataset), transformationType).take(3)
            }
            case other => new IllegalArgumentException(s"operation $other is not supported as of now")
          }

        })
        exprDataset
      }

      val expectedResult = {
        val r1: Row = Row("a", 10, 100, "dehradun", "TEENAGER", "dehradun-OTHERS")
        val r2: Row = Row("b", 20, 200, "noida", "ADULT", "noida-NCR")
        val r3: Row = Row("ca", 30, 400, "gurgaon", "ADULT", "gurgaon-NCR")
        Array(r1, r2, r3)
      }
      actualResult shouldBe expectedResult
    }

  }
}