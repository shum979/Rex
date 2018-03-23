package reader

import common.TestSparkContextProvider
import org.apache.spark.sql.Row
import org.scalatest.{Matchers, WordSpec}
import rex.core.xml.DataSourceType
import rex.src.reader.DatasetReader

/**
  * Created by Shubham Gupta on 12/6/2017.
  */

class ReaderTest extends WordSpec with Matchers with TestSparkContextProvider {


  "CsvReader" should {
    "read data from csv file and schema from schema file " in {
      val fullpath = this.getClass.getResource("/data/seller_data.txt").getPath

      val inputdir = fullpath.substring(0, fullpath.lastIndexOf("/"))
      val inputFileName = fullpath.substring(fullpath.lastIndexOf("/") + 1, fullpath.length)

      val schemaPath: String = this.getClass.getResource("/data/seller_data_schema.json").getPath

      val sourceManifest =
        raw"""  <DataSource type="file" name="file_source">
                                      <File>
                                        <DirectoryPath>$inputdir</DirectoryPath>
                                         <FileNameOrPattern>$inputFileName</FileNameOrPattern>
                                         <TopLinesToIgnore>0</TopLinesToIgnore>
                                         <BottomLinesToIgnore>0</BottomLinesToIgnore>
                                         <Delimiter>|</Delimiter>
                                         <SchemaFilePath>$schemaPath</SchemaFilePath>
                                         <Tags>filesystem=FI,valueDate=05022018</Tags>
                                      </File>
                                   </DataSource>"""

      val result = scalaxb.fromXML[DataSourceType](scala.xml.XML.loadString(sourceManifest))

      val reader = DatasetReader(result.datasourcetypeoption)
      val dataset = reader.readData
      val row = dataset.take(1)(0)
      val treeString = row.schema.treeString
      val count = dataset.count
      count shouldBe 400
      val rows = dataset.take(1)
      val string = rows.mkString(",")
      string shouldBe "[9001,Institutional,High,25-Jul-2014,1.3,2017-03-29 16:02:25,124]"

      dataset.metadata should contain("filesystem" -> "FI")
      dataset.metadata should contain("valueDate" -> "05022018")
    }


    "read csv with proper schema given in schema tag" in {
      val fullpath = this.getClass.getResource("/data/seller_data.txt").getPath
      val inputdir = fullpath.substring(0, fullpath.lastIndexOf("/"))
      val inputFileName = fullpath.substring(fullpath.lastIndexOf("/") + 1, fullpath.length)

      val sourceManifest =
        raw"""  <DataSource type="file" name="file_source">
                                      <File>
                                        <DirectoryPath>$inputdir</DirectoryPath>
                                         <FileNameOrPattern>$inputFileName</FileNameOrPattern>
                                         <Delimiter>|</Delimiter>
                                         <Schema>
                                            <SchemaColumn dataType="integerType">productId</SchemaColumn>
                                            <SchemaColumn dataType="stringType">sellerType</SchemaColumn>
                                            <SchemaColumn dataType="stringType">sellerValue</SchemaColumn>
                                            <SchemaColumn dataType="stringType">enrollmentDate</SchemaColumn>
                                          </Schema>
                                         <Tags>filesystem=FI,valueDate=05022018</Tags>
                                      </File>
                                   </DataSource>"""

      val result = scalaxb.fromXML[DataSourceType](scala.xml.XML.loadString(sourceManifest))

      val reader = DatasetReader(result.datasourcetypeoption)
      val dataset = reader.readData
      val schema = dataset.schema().simpleString
      schema shouldBe "struct<productId:int,sellerType:string,sellerValue:string,enrollmentDate:string>"
    }

    "throw exception for not providing schema" in {
      val fullpath = this.getClass.getResource("/data/seller_data.txt").getPath
      val inputdir = fullpath.substring(0, fullpath.lastIndexOf("/"))
      val inputFileName = fullpath.substring(fullpath.lastIndexOf("/") + 1, fullpath.length)

      val schemaPath: String = this.getClass.getResource("/data/seller_data_schema.json").getPath

      val sourceManifest =
        raw"""  <DataSource type="file" name="file_source">
                                      <File>
                                        <DirectoryPath>$inputdir</DirectoryPath>
                                         <FileNameOrPattern>$inputFileName</FileNameOrPattern>
                                         <Delimiter>|</Delimiter>
                                      </File>
                                   </DataSource>"""

      val result = scalaxb.fromXML[DataSourceType](scala.xml.XML.loadString(sourceManifest))

      val reader = DatasetReader(result.datasourcetypeoption)
      val exception = the[IllegalStateException] thrownBy reader.readData
      exception.getMessage should startWith("No schema is provided for datasource ")
    }
  }

  "CsvReader" should {
    "read files with specific patterns from directory" in {
      val fullpath = this.getClass.getResource("/dataDirectory").getPath

      val sourceManifest =
        raw"""<DataSource type="file" name="source_file">
           <File>
               <DirectoryPath>$fullpath</DirectoryPath>
               <FileNameOrPattern>*.env</FileNameOrPattern>
                <Delimiter>=</Delimiter>
               <Schema>
                   <SchemaColumn dataType="stringType" isNullable="true">key</SchemaColumn>
                   <SchemaColumn dataType="stringType">value</SchemaColumn>
               </Schema>
           </File>
          </DataSource>"""

      val result = scalaxb.fromXML[DataSourceType](scala.xml.XML.loadString(sourceManifest))

      val reader = DatasetReader(result.datasourcetypeoption)
      val dataset = reader.readData

      dataset.show()

      val actualdata = dataset.collect()

      val expectedData = Array(Row("BASE_PLS", "RELPLS"),
        Row("BASE_PLS_SCN", "RELSCNPLS"),
        Row("BASE_SUMMIT", "UATESUM01"),
        Row("Bitversion", "64"))

      actualdata should equal(expectedData)
    }


    "append extra columns to dataset " in {
      val fullpath = this.getClass.getResource("/dataDirectory").getPath

      val sourceManifest =
        raw"""<DataSource type="file" name="source_file">
           <File>
               <DirectoryPath>$fullpath</DirectoryPath>
               <FileNameOrPattern>*.env</FileNameOrPattern>
                <Delimiter>=</Delimiter>
               <Schema>
                   <SchemaColumn dataType="stringType" isNullable="true">key</SchemaColumn>
                   <SchemaColumn dataType="stringType">value</SchemaColumn>
               </Schema>
              <Tags>filesystem=FI,valueDate=05022018</Tags>
             <WithColumn name="fileName">$$FILE_NAME</WithColumn>
             <WithColumn name="filesystem">$$filesystem</WithColumn>
             <WithColumn name="constant">shubham</WithColumn>
           </File>
          </DataSource>"""

      val result = scalaxb.fromXML[DataSourceType](scala.xml.XML.loadString(sourceManifest))

      val reader = DatasetReader(result.datasourcetypeoption)
      val dataset = reader.readData

      val actualdata = dataset.collect()
    }
  }

  "FileReader" should {
    "read files with specific patterns from directory" in {
      val fullpath = this.getClass.getResource("/data/input_data.parquet").getPath

      val sourceManifest =
        raw"""<DataSource type="file" name="source_file">
           <File format="parquet">
               <DirectoryPath>$fullpath</DirectoryPath>
           </File>
          </DataSource>"""

      val result = scalaxb.fromXML[DataSourceType](scala.xml.XML.loadString(sourceManifest))
      val reader = DatasetReader(result.datasourcetypeoption)
      val dataset = reader.readData
      dataset.count shouldBe 5
      dataset.take(1).mkString(",") should equal("[9001,Institutional,High,25-Jul-2014,1.3,2017-03-29 16:02:25,124]")
    }
  }

}
