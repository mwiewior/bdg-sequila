package org.biodatageeks.datasources.BAM

import org.apache.log4j.Logger
import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext.VariantContext
import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.{Encoders, Row, RowFactory, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{BooleanType, StructType}
import org.disq_bio.disq.{HtsjdkReadsRddStorage, HtsjdkVariantsRddStorage}


class VCFRelation(path: String)(@transient val sqlContext: SQLContext) extends BaseRelation
  with PrunedScan
  with Serializable
  with Logging{

  val spark = sqlContext.sparkSession


  lazy val df =  spark.read
    .format("com.lifeomic.variants")
    .option("use.format.type", "false")
    .load(path)
    .withColumnRenamed("sampleid","sample_id")
    .withColumnRenamed("chrom", "contig")




  override def schema: org.apache.spark.sql.types.StructType = {
   df.schema
  }

  override def buildScan(requiredColumns: Array[String] ) = {

    {
      if (requiredColumns.length > 0)
        df.select(requiredColumns.head, requiredColumns.tail: _*)
      else
        df
    }.rdd


  }

}
