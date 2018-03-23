package rex.main

import org.scalatest.{Matchers, WordSpec}

/**
  * Created by Shubham Gupta on 01-Mar-18.
  */
class UserPropertyHandlerTest extends WordSpec with Matchers {


  "rex.main.UserPropertyHandlerTest" should {
    "read key value properties from file " in {

      /*
            val projectPath = System.getProperty("user.dir")
            val xml  = raw"""<PropertyFilePath propertyWithPrefix="shubham">$projectPath/src/test/resources/testData/seller_data.env</PropertyFilePath>"""
            val config = scalaxb.fromXML[PropertyFilePathType](scala.xml.XML.loadString(xml))

            println(config.propertyWithPrefix.get)
            UserPropertyHandler.getPropertyMapFromFile(config).foreach(println)
      */

    }

  }


}
