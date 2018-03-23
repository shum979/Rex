package rex.core.xml

import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory

import scala.util.Try

/**
  * Created by Shubham Gupta on 01-Mar-18.
  */
object XmlValidator {

  def validate(xmlFile: String, xsdFile: String): Boolean = {

    Try({
      val schemaLang = "http://www.w3.org/2001/XMLSchema"
      val factory = SchemaFactory.newInstance(schemaLang)
      val schema = factory.newSchema(new StreamSource(xsdFile))
      val validator = schema.newValidator()
      validator.validate(new StreamSource(xmlFile))
      true
    }).getOrElse(false)
  }

}
