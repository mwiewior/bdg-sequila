package org.biodatageeks.sequila.pileup

import htsjdk.samtools.SAMRecord
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.biodatageeks.sequila.datasources.BAM.BDGAlignFileReaderWriter
import org.biodatageeks.sequila.datasources.InputDataType
import org.biodatageeks.sequila.inputformats.BDGAlignInputFormat
import org.biodatageeks.sequila.utils.{DataQualityFuncs, TableFuncs}
import org.seqdoop.hadoop_bam.CRAMBDGInputFormat

import scala.reflect.ClassTag


case class PileupRecord (contig: String, pos: Int, ref: String, cov: Short, countRef:Short, countNonRef:Short)


class Pileup[T<:BDGAlignInputFormat](spark:SparkSession)(implicit c: ClassTag[T]) extends BDGAlignFileReaderWriter[T] {
  val logger: Logger = Logger.getLogger(this.getClass.getCanonicalName)

  def handlePileup(tableName: String, output: Seq[Attribute]): RDD[PileupRecord] = {
    logger.info(s"Calculating pileup on table: $tableName")

    lazy val alignments = readTableFile(name=tableName)
    logger.info(s"Processing ${alignments.count()} reads in total" )

    val out = calculatePileup(alignments)
    out
  }

  private def calculatePileup(alignments:RDD[SAMRecord]): RDD[PileupRecord] = {

    // dummy implementation v 0.1
    //    val rdd = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    //    val out = rdd.map(r => PileupRecord("1", r, "A", (r + 10).toShort, (r + 9).toShort, (r + 1).toShort))

    // dummy implementation v 0.2

    alignments.map(read => PileupRecord(
      DataQualityFuncs.cleanContig(read.getContig),
      read.getAlignmentStart,
      read.getReadString()(0).toString,
      10.toShort, 8.toShort, 2.toShort))
  }


  private def readTableFile(name: String): RDD[SAMRecord] = {
    val metadata = TableFuncs.getTableMetadata(spark, name)
    val path = metadata.location.toString

    metadata.provider match {
      case Some(f) =>
        if (f == InputDataType.BAMInputDataType)
           readBAMFile(spark.sqlContext, path, refPath = None)
        else if (f == InputDataType.CRAMInputDataType) {
          val refPath = spark.sqlContext
            .sparkContext
            .hadoopConfiguration
            .get(CRAMBDGInputFormat.REFERENCE_SOURCE_PATH_PROPERTY)
           readBAMFile(spark.sqlContext, path, Some(refPath))
        }
        else throw new Exception("Only BAM and CRAM file formats are supported in bdg_coverage.")
      case None => throw new Exception("Wrong file extension - only BAM and CRAM file formats are supported in bdg_coverage.")
    }
  }
}
