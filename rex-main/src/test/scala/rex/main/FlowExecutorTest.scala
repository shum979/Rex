package rex.main

import java.io.PrintWriter

import org.apache.spark.sql.Row
import org.scalatest.{Matchers, WordSpec}
import rex.core.common.SparkUtils
import rex.main.manifest.DataFlowExecutor

import scala.io.Source

/**
  * Created by Shubham Gupta on 22/01/2017.
  */

class FlowExecutorTest extends WordSpec with Matchers with TestSparkContextProvider {

  val projectPath = System.getProperty("user.dir")
  val inMap = Map("PROJECT_PATH" -> projectPath)


  "FlowExecutor" should {
    "read, transform and store data" in {
      val outPath = "target/first_executor_flow_test_1.xml"
      prepareInputFile("/xmls/first_executor_flow_test.xml", inMap, outPath)

      DataFlowExecutor.execute(s"$projectPath/$outPath")

      val output = spark.read.csv(s"$projectPath/target/sampleOut")

      val count = output.count()
      val rows = output.take(1)
      val string = rows.mkString(",")

      count shouldBe 5
      string shouldBe "[9003,OTHER,Impactful,07-Apr-2013,0.8,2017-04-12 16:10:31,130]"

    }

  }

  "FlowExecutor" should {
    "read, recon and store data" in {
      val outPath = "target/recon_sample_1.xml"
      prepareInputFile("/xmls/recon_sample.xml", inMap, outPath)

      DataFlowExecutor.execute(s"$projectPath/$outPath")

      val output = spark.read.csv(s"$projectPath/target/ReconciliationFlow")
      val count = output.count()

      count shouldBe 5
    }

    "should set user defined properties in spark context" in {

      val outPath = "target/recon_sample_2.xml"
      prepareInputFile("/xmls/recon_sample.xml", inMap, outPath)

      DataFlowExecutor.execute(s"$projectPath/$outPath")

      SparkUtils.getPropertyValue("AppWriterName").get shouldBe "Shubham Gupta"
      SparkUtils.getPropertyValue("AppWritingPlace").get shouldBe "Gurgaon"
    }
  }

  "FlowExecutor" should {
    "read,groupby,join and store data" in {

      val outPath = "target/grp_and_join_sample.xml"
      prepareInputFile("/xmls/grp_and_join_sample.xml", inMap, outPath)

      DataFlowExecutor.execute(s"$projectPath/$outPath")
      val output = spark.read.csv(s"$projectPath/target/competition_analysis")
      output.count() shouldBe 9

      val expectedOutput = Array(Row("45442374", "2", "45442374", "143", "1431984", "2441.16", "24.09", "26.39", "9005", "2017-05-01 16:10:05"),
        Row("63786748", "1", "63786748", "193", "1931662", "2073.70", "6.36", "17.88", "9002", "2017-05-07 15:51:20"),
        Row("82545658", "3", "82545658", "187", "1871429", "373.58", "23.14", "35.27", "9005", "2017-04-27 15:45:04"),
        Row("43157024", "3", "43157024", "177", "1771787", "1272.39", "9.92", "18.59", "9002", "2017-04-30 16:10:54"),
        Row("36130457", "2", "36130457", "160", "1601138", "890.75", "5.62", "28.86", "9003", "2017-04-28 15:58:58"))

      output.take(5) should equal(expectedOutput)
    }
  }


  "FlowExecutor" should {
    "append columns from env file in data file" in {

      val outPath = "target/complete_business_flow_sample.xml"
      prepareInputFile("/xmls/complete_business_flow_sample.xml", inMap, outPath)

      DataFlowExecutor.execute(s"$projectPath/$outPath")
      val output = spark.read.csv(s"$projectPath/target/fullBussinessFlow")
      val resultantRowArray = output.collect()

      resultantRowArray.length shouldBe 2

      val row1 = resultantRowArray.apply(0)

      row1.getAs[String](7) shouldBe "PARIS"
      row1.getAs[String](22) shouldBe "UATESUM01"
      row1.getAs[String](15) shouldBe "SWAP"
    }
  }


  def prepareInputFile(inputFile: String, configMap: Map[String, String], outFile: String): Unit = {
    val fullpath = this.getClass.getResource(inputFile).getPath

    val xml = Source.fromFile(fullpath).getLines().map { line =>
      var resolvedXmlStr = line
      configMap.foreach(entry => {
        resolvedXmlStr = resolvedXmlStr.replace("$" + entry._1, entry._2)
      })
      resolvedXmlStr
    } toList

    val writer = new PrintWriter(s"$projectPath/$outFile")
    writer.write(xml.mkString)
    writer.close()
  }
}
