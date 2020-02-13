package org.biodatageeks.sequila.pileup

import htsjdk.samtools.SAMRecord
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.biodatageeks.sequila.datasources.BAM.BDGAlignFileReaderWriter
import org.biodatageeks.sequila.datasources.InputDataType
import org.biodatageeks.sequila.inputformats.BDGAlignInputFormat
import org.biodatageeks.sequila.utils.{DataQualityFuncs, InternalParams, TableFuncs}
import org.seqdoop.hadoop_bam.CRAMBDGInputFormat

import scala.reflect.ClassTag


case class PileupRecord (contig: String, pos: Int, ref: String, cov: Short, countRef:Short, countNonRef:Short)


class Pileup[T<:BDGAlignInputFormat](spark:SparkSession)(implicit c: ClassTag[T]) extends BDGAlignFileReaderWriter[T] {
  val logger: Logger = Logger.getLogger(this.getClass.getCanonicalName)

  def handlePileup(tableName: String, output: Seq[Attribute]): RDD[PileupRecord] = {
    logger.info(s"Calculating pileup on table: $tableName")

    lazy val allAlignments = readTableFile(name=tableName)
    logger.info(s"Processing ${allAlignments.count()} reads in total" )

    val alignments = filterAlignments(allAlignments)

    val out = PileupMethods.calculatePileup(alignments)
    out
  }


  private def filterAlignments(alignments:RDD[SAMRecord]): RDD[SAMRecord] = {
    // any other filtering conditions should go here
    val filterFlag = spark.conf.get(InternalParams.filterReadsByFlag, "1796").toInt
    val cleaned = alignments.filter(read => read.getContig != null && (read.getFlags & filterFlag) == 0)
    logger.info(s"Processing ${cleaned.count()} cleaned reads in total" )
    cleaned


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
