package transform

import common.TestSparkContextProvider
import org.scalatest.{Matchers, WordSpec}
import rex.core.common.ManagedDataset
import rex.core.xml.TransformationType
import rex.src.transform.ColumnTransformer

import scala.xml.XML

/**
  * Created by Shubham Gupta on 27-Feb-18.
  */
class ColumnTransformerTest extends WordSpec with Matchers with TestSparkContextProvider with Serializable {

  "ColumnTransformer" should {
    "add column to dataset" in {

      val grpbyXml =
        """<Transformation name="columnTransformation" type="ColumnTransformation" applyondataref="inputDataset" >
          |    <ColumnTransformations>
          |        <AddNew>
          |            <Column name="sourceidentifier" dataType="stringType" valueFrom="fromStandardEnvVariable">input_filename</Column>
          |        </AddNew>
          |        <Delete>
          |             <Column>LastsoldTs</Column>
          |       </Delete>
          |       <Rename>
          |            <Column existingName="SellerType" newName="classification"></Column>
          |       </Rename>
          |    </ColumnTransformations>
          |</Transformation>"""

      val columnTransfrormConfigs = scalaxb.fromXML[TransformationType](XML.loadString(grpbyXml))

      val sellerDataset: ManagedDataset = managedDatasetFromFile("/data/reporting_data.txt")
      val managedDataset = sellerDataset.copy(metadata = Map("input_filename" -> sellerDataset.getFileNames()))

      val result = ColumnTransformer.transform(Map("inputDataset" -> managedDataset), columnTransfrormConfigs)
      result.count shouldBe 5

      val resultantschema = result.schema().simpleString
      println(resultantschema)
      resultantschema contains ("sourceidentifier") shouldBe true
      resultantschema contains ("classification") shouldBe true
      resultantschema contains ("SellerType") shouldBe false

    }
  }
}

