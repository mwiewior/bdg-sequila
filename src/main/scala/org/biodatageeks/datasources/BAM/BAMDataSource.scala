package org.biodatageeks.datasources.BAM

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
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


  def saveAsBAMFile(spark: SparkSession, parameters: Map[String, String], mode: SaveMode,data: DataFrame) = {

    val outPath = parameters("path")
    import spark.implicits._

    val schema  = data.schema
    val ds = data
        .as[BDGSAMRecord]


    println(ds.count())
    println(schema.treeString)


  }

  //CTAS
  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
                              parameters: Map[String, String], data: DataFrame): BaseRelation = {


    //println(sqlContext.getConf(BDGInternalParams.BAMCTASDir))
    println("BAMDataSource writer")
    val spark = sqlContext.sparkSession
    //saveAsBAMFile(spark, parameters, mode, data)
    createRelation(sqlContext, parameters)
  }

  //IAS
  override def insert(data: DataFrame, overwrite: Boolean) = ???

}