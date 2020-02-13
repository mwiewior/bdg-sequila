package org.biodatageeks.sequila.pileup

import htsjdk.samtools.SAMRecord
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.biodatageeks.sequila.utils.DataQualityFuncs

object PileupMethods {

  val logger: Logger = Logger.getLogger(this.getClass.getCanonicalName)

  def calculatePileupMock(alignments:RDD[SAMRecord]):RDD[PileupRecord] = {

    alignments.map(read => PileupRecord(
      DataQualityFuncs.cleanContig(read.getContig),
      read.getAlignmentStart,
      read.getReadString()(0).toString,
      10.toShort, 8.toShort, 2.toShort))
  }

  def calculatePileup(alignments:RDD[SAMRecord]):RDD[PileupRecord] = {



    alignments.map(read => PileupRecord(
      DataQualityFuncs.cleanContig(read.getContig),
      read.getAlignmentStart,
      read.getReadString()(0).toString,
      10.toShort, 8.toShort, 2.toShort))
  }

  def collectEvents(alignments:RDD[SAMRecord]) ={

  }

}
