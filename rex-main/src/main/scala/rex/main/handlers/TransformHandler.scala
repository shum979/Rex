package rex.main.handlers

import rex.core.common.ManagedDataset
import rex.core.xml.TransformationsType
import rex.src.transform.Transformer

import scala.collection.mutable

/**
  * Created by Shubham Gupta on 13-Jan-18.
  */
object TransformHandler {


  /**
    * method for binding transform configuration with it's specific implementations
    *
    * @param referenceMap
    * @param transformations -- transform configurations as coming from xml file.
    */
  def transform(referenceMap: mutable.Map[String, ManagedDataset], transformations: TransformationsType) = {

    transformations.Transformation.foreach(transformationsType => {
      val result = Transformer(transformationsType.transCategory)
        .transform(referenceMap.toMap, transformationsType)
      referenceMap += (transformationsType.name -> result)
    })
  }

}

