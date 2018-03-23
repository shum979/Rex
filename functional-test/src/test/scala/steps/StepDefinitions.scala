package steps

import cucumber.api.scala.{EN, ScalaDsl}
import org.scalatest.Matchers
import rex.main.manifest.DataFlowExecutor
import utils.StandardConstants._
import utils.{FileResolver, Helper, SparkResultDatasetBean, TestSparkContextProvider}

import scala.collection.mutable

class StepDefinitions extends ScalaDsl with EN with Matchers with TestSparkContextProvider {

  val propertiesMap = mutable.Map[String, String]()
  val dfReader = spark.read

  var currentTestCaseName: String = _
  var generalResult: SparkResultDatasetBean = _
  var additionalResult4Recon: SparkResultDatasetBean = _


  Given("""^environment for (\w+) is ready$""") { (testCaseName: String) =>
    propertiesMap += (TEST_DATA_PATH_STRING -> TEST_DATA_PATH_VALUE)
    propertiesMap += (APP_OUTPUT_PATH_STRING -> getAppOutputPathValue(testCaseName))
    propertiesMap += (TEST_CASE_STRING -> testCaseName)
    currentTestCaseName = testCaseName;
  }

  //------------------------Implementing When clause ---------------------------------

  When("""^configs are given by (\w+\.xml)""") { (fileName: String) =>
    val resolvedXmlPath = new FileResolver(fileName, currentTestCaseName) readAndResolveProperties (propertiesMap) thenWrite()
    DataFlowExecutor.execute(resolvedXmlPath)
  }

  //------------------------Implementing Then clause ---------------------------------

  Then("""^result should produce (\d+) rows$""") { (expectedResult: Int) =>
    val jobOutputPath = propertiesMap(APP_OUTPUT_PATH_STRING)
    generalResult = SparkResultDatasetBean(dfReader.csv(jobOutputPath).collect())

    generalResult.rowArray.length shouldBe expectedResult
  }

  Then("""^recon should produce (\d+) rows at field level and (\d+) rows at record level$""") { (expectedFieldCnt: Int, expectedRecordsCnt: Int) =>
    val jobOutputPath = propertiesMap(APP_OUTPUT_PATH_STRING)

    generalResult = SparkResultDatasetBean(dfReader.csv(jobOutputPath + "/field_level").collect())
    additionalResult4Recon = SparkResultDatasetBean(dfReader.csv(jobOutputPath + "/record_level").collect())

    generalResult.rowArray.length shouldBe expectedFieldCnt
    additionalResult4Recon.rowArray.length shouldBe expectedRecordsCnt
  }


  //------------------------Implementing And clause ---------------------------------

  And("""^first record should be (.+)$""") { (expectedResult: String) =>
    val row = generalResult.rowArray(0)
    val string = row.mkString(",")
    string shouldBe expectedResult
  }


  And("""^result record should contain (.+)$""") { (expectedResult: String) =>
    val row = generalResult.rowArray(0)
    val string = row.mkString(",")
    assert(string.startsWith(expectedResult))
  }

  And("""^output should be same as specified in file (.+)$""") { (expectedOutputFile: String) =>
    val absFilePath = getTestDataExpectedDir(currentTestCaseName, expectedOutputFile)
    val expectedArr = Helper.fileToRowArray(absFilePath)
    generalResult.rowArray should equal(expectedArr)
  }


  //------------------------Implementing other supporting methods ---------------------------------

}