package org.biodatageeks.datasources.BAM

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}

class VCFDataSource extends DataSourceRegister
  with RelationProvider {

  override def shortName(): String = "VCF"
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    new VCFRelation(parameters("path"))(sqlContext)
  }

}
