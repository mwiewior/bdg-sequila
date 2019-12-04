package org.biodatageeks.sequila.datasources.VCF

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources._
import org.biodatageeks.sequila.utils.{Columns, DataQualityFuncs}
import org.apache.spark.sql.functions._




class VCFRelation(path: String)(@transient val sqlContext: SQLContext) extends BaseRelation
  with PrunedScan
  with Serializable
  with Logging{

  val spark: SparkSession = sqlContext.sparkSession

  val cleanContigUDF = udf[String,String](DataQualityFuncs.cleanContig)

  lazy val inputDf: DataFrame =  spark
    .read
    .format("vcf")
    .option("splitToBiallelic", "true")
    .load(path)
    .withColumnRenamed("contigName", Columns.CONTIG)
    .withColumnRenamed("start", Columns.START)
    .withColumnRenamed("end", Columns.END)
    .withColumnRenamed("referenceAllele", Columns.REF)
    .withColumnRenamed("alternateAlleles", Columns.ALT)

  lazy val df = inputDf
    .withColumn(Columns.CONTIG, cleanContigUDF(inputDf(Columns.CONTIG)))

  override def schema: org.apache.spark.sql.types.StructType = {
   df.schema
  }

  override def buildScan(requiredColumns: Array[String] ): RDD[Row] = {

    {
      if (requiredColumns.length > 0)
        df.select(requiredColumns.head, requiredColumns.tail: _*)
      else
        df
    }.rdd


  }

}
