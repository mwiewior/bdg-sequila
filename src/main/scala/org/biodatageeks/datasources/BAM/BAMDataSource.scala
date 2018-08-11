package org.biodatageeks.datasources.BAM

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources._
import org.biodatageeks.utils.BDGInternalParams
import org.seqdoop.hadoop_bam.BAMBDGInputFormat


class BAMDataSource extends DataSourceRegister
  with RelationProvider
  with CreatableRelationProvider
  with InsertableRelation{
  override def shortName(): String = "BAM"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {

    new BDGAlignmentRelation[BAMBDGInputFormat](parameters("path"))(sqlContext)
  }

  //CTAS
  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
                              parameters: Map[String, String], data: DataFrame): BaseRelation = {


    println(sqlContext.getConf(BDGInternalParams.BAMCTASDir))
    createRelation(sqlContext, parameters)
  }

  //IAS
  override def insert(data: DataFrame, overwrite: Boolean) = ???

}