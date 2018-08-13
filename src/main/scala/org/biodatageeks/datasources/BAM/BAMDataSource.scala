package org.biodatageeks.datasources.BAM

import htsjdk.samtools.SAMRecord
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.sources._
import org.biodatageeks.utils.{BDGInternalParams, BDGSerializer, BDGTableFuncs}
import org.seqdoop.hadoop_bam.BAMBDGInputFormat


class BAMDataSource extends DataSourceRegister
  with RelationProvider
  with CreatableRelationProvider
  with InsertableRelation
  with BDGAlignFileReaderWriter[BAMBDGInputFormat] {
  override def shortName(): String = "BAM"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {

    new BDGAlignmentRelation[BAMBDGInputFormat](parameters("path"))(sqlContext)
  }


  def save(spark: SparkSession, parameters: Map[String, String], mode: SaveMode, data: DataFrame) = {

    import spark.implicits._
    val ds = data
      .as[BDGSAMRecord]
    val sampleName = ds.first().sampleId
    val samplePath = s"${parameters(BDGInternalParams.BAMCTASDir)}/${sampleName}*.bam"
    val srcBAMRDD =
      ds
        .rdd
        .map( r => BDGSerializer.deserialise(r.SAMRecord.get).asInstanceOf[SAMRecord] )
    val headerPath = BDGTableFuncs.getExactSamplePath(spark,samplePath)
    saveAsBAMFile(spark.sqlContext,srcBAMRDD,s"${parameters("path").split('/').dropRight(1).mkString("/")}/${sampleName}.bam",headerPath)
  }

  //CTAS
  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
                              parameters: Map[String, String], data: DataFrame): BaseRelation = {


    val spark = sqlContext.sparkSession
    save(spark, parameters, mode ,data)
    createRelation(sqlContext, parameters)
  }

  //IAS
  override def insert(data: DataFrame, overwrite: Boolean) = ???

}