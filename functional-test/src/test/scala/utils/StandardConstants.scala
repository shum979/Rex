package utils

import java.nio.file.Paths

/**
  * Created by Shubham Gupta on 21-Feb-18.
  * This is class to hold constants for this module
  */
object StandardConstants {

  val TEST_DATA_PATH_STRING = "TEST_DATA_PATH"
  val APP_OUTPUT_PATH_STRING = "APP_OUTPUT_PATH"
  val TEST_CASE_STRING = "TEST_CASE"
  val PROJECT_PATH = System.getProperty("user.dir")
  val STD_TEMP_PROP_RESOLVED_DIR = Paths.get(PROJECT_PATH, "target", "resolved") toString

  def TEST_DATA_PATH_VALUE = Paths.get(PROJECT_PATH, "testData") toString

  def getAppOutputPathValue(currentTestCase: String) = Paths.get(PROJECT_PATH, "target", currentTestCase) toString()

  def getStdTestXmlPath(testCaseName: String, fileName: String): String = Paths.get(PROJECT_PATH, "testData", testCaseName, "config", fileName) toString

  def getTestDataExpectedDir(testCaseName: String, fileName: String) = Paths.get(PROJECT_PATH, "testData", testCaseName, "expectedData", fileName) toString

}
