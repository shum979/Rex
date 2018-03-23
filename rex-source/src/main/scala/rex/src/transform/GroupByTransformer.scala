package rex.src.transform

import rex.core.common.ManagedDataset
import rex.core.xml.{AggregationType, TransformationType}

/**
  * Created by Shubham Gupta on 24-Feb-18.
  */
object GroupByTransformer extends Transformer {


  // helper lambda function for replacing spark-generated name with given alias
  private val aggColumnName: (AggregationType) => (String, String) = { aggregation =>
    val generatedName = aggregation.aggregator.toString.toLowerCase + "(" + aggregation.value + ")"
    (generatedName -> aggregation.alias)
  }

  /**
    * this method implements group by functionality
    *
    * @param datasetMap -- contains the list of managedDataset needed for this operation
    * @param config     -- this is configuration for GroupBy transformation
    * @return ManagedDataset
    */
  override def transform(datasetMap: Map[String, ManagedDataset], config: TransformationType): ManagedDataset = {
    val groupByConfig = config.GroupByTransformation.get
    val managedDataset = datasetMap(config.applyondataref)

    val columns = groupByConfig.GroupByColumn.map(column => managedDataset.apply(column))
    val aggregateFunctionMap = groupByConfig.AggregateColumn.map(agg => (agg.value, agg.aggregator.toString.toLowerCase())) toMap

    val columnAliasMap = groupByConfig.AggregateColumn.map(aggColumnName) toMap

    var result = managedDataset.groupBy(aggregateFunctionMap, columns)
    columnAliasMap.foreach { case (existingName, newName) =>
      result = result.withColumnRenamed(existingName, newName)
    }
    return result
  }
}
