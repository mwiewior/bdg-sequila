package org.biodatageeks.datasources.BAM

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.sources._
import org.biodatageeks.utils.{BDGInternalParams, BDGTableFuncs}
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


  def save(spark: SparkSession, parameters: Map[String, String], mode: SaveMode) = {

    println(parameters.mkString("|"))
    val outPath = parameters("path")
    val sampleName = "NA12878"
    val samplePath = s"${parameters(BDGInternalParams.BAMCTASDir)}/${sampleName}*.bam"
    val srcBAMRDD = readBAMFile(spark.sqlContext,samplePath)
    println(s"${parameters("path").split('/').dropRight(1).mkString("/")}/${sampleName}.bam")
    val headerPath = BDGTableFuncs.getExactSamplePath(spark,samplePath)
    saveAsBAMFile(spark.sqlContext,srcBAMRDD,s"${parameters("path").split('/').dropRight(1).mkString("/")}/${sampleName}.bam",headerPath)
    println(srcBAMRDD.count())






  }

  //CTAS
  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
                              parameters: Map[String, String], data: DataFrame): BaseRelation = {


    val spark = sqlContext.sparkSession
    save(spark, parameters, mode)

    createRelation(sqlContext, parameters)
  }

  //IAS
  override def insert(data: DataFrame, overwrite: Boolean) = ???

}