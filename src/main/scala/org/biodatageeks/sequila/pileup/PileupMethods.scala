package org.biodatageeks.sequila.pileup

import htsjdk.samtools.{SAMRecord, SAMSequenceRecord}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.biodatageeks.sequila.utils.DataQualityFuncs

import scala.collection.{JavaConverters, mutable}


case class TraversalAggregate (
  contig: String,
  contigLen: Int,
  covArray: Array[Short],
  startPosition: Int,
  maxPosition: Int,
  maxCigar: Int)


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

    val contigLenMap = initializeContigLengths(alignments)


    alignments.map(read => PileupRecord(
      contigLenMap.keysIterator.next(),
      read.getAlignmentStart,
      read.getReadString()(0).toString,
      10.toShort, 8.toShort, 2.toShort))
  }

  def initializeContigLengths(alignments:RDD[SAMRecord]) = {
    val contigLenMap = new mutable.HashMap[String, Int]()

    val sequenceList = alignments.first().getHeader().
      getSequenceDictionary.getSequences()
    val sequenceSeq = JavaConverters.asScalaIteratorConverter(sequenceList.iterator()).asScala.toSeq

    for (sequence <- sequenceSeq) {
      logger.debug("sequence name {} => {} ", sequence.getSequenceName(), sequence.getSequenceLength() )
      val contigName = DataQualityFuncs.cleanContig(sequence.getSequenceName)
      contigLenMap += contigName -> sequence.getSequenceLength
    }
    contigLenMap.toMap
  }

//  def collectEvents(alignments:RDD[SAMRecord]): RDD[TraversalAggregate] = {
//
//  }

}
