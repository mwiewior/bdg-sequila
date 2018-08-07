package org.biodatageeks.datasources.BAM

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.seqdoop.hadoop_bam.BAMBDGInputFormat


class BAMDataSource extends DataSourceRegister with RelationProvider {
  override def shortName(): String = "BAM"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation =
    new BAMRelation[BAMBDGInputFormat](parameters("path"))(sqlContext)
}