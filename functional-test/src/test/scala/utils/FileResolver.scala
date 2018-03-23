package utils

import java.io.{File, PrintWriter}

import utils.StandardConstants._

import scala.io.Source

/**
  * Created by Shubham Gupta on 17-Feb-18.
  */

/** this class is a utility class for resolving properties(like $PROJECT_PATH) in xml file
  * It is just needed for testing.
  * It helps in converting project relative paths to absolute paths, so spark can work on them
  *
  * @param fileName -- name of xml file to be loaded for given testcase, it is assumed to be sitting in
  * @param testCase -- this is name of current test case
  *                 input xml file is supposed to sitting at /testData/testCase/config/fileName.xml
  */
class FileResolver(fileName: String, testCase: String) {

  var fileString: String = _

  /*this method coverts relative path of input xml to actual path
   * it also replaces property in xml to their value with help of given map  */
  def readAndResolveProperties(implicit inMap: collection.Map[String, String]) = {
    val res = Source.fromFile(getStdTestXmlPath(testCase, fileName)).getLines map (replaceProperty)
    fileString = res.mkString("")
    this
  }

  /*delegate method for replacing properties in line with help of given map */
  private def replaceProperty(line: String)(implicit configMap: collection.Map[String, String]) = {
    var resolvedString = line
    configMap.foreach { case (key, value) => {
      resolvedString = resolvedString.replace("$" + key, value)
    }
    }
    resolvedString
  }


  /*It writes new config xml file after replacing all it's properties with respective values
  * It is needed so that given xml remain as it*/
  def thenWrite(): String = {
    val targetDir = new File(STD_TEMP_PROP_RESOLVED_DIR)
    if (!targetDir.exists()) {
      targetDir.mkdir()
    }

    val outPath = STD_TEMP_PROP_RESOLVED_DIR + File.separator + fileName
    val writer = new PrintWriter(outPath)
    writer.write(fileString)
    writer.close()
    return outPath
  }

}
