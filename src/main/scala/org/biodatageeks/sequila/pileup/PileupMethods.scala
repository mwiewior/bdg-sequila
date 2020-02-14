package org.biodatageeks.sequila.pileup

import htsjdk.samtools.SAMRecord
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.biodatageeks.sequila.utils.DataQualityFuncs
import scala.collection.{JavaConverters, mutable}




/**
  * Class implementing pileup calculations on set of aligned reads
  */
object PileupMethods {

  val logger: Logger = Logger.getLogger(this.getClass.getCanonicalName)

  /**
    * mock implementation of pileup
    * @param alignments aligned reads
    * @return distributed collection of PileupRecords
    */
  def calculatePileupMock(alignments:RDD[SAMRecord]): RDD[PileupRecord] = alignments.map(read => PileupRecord(
    DataQualityFuncs.cleanContig(read.getContig),
    read.getAlignmentStart,
    ref = read.getReadString()(0).toString,
    cov = 10.toShort, countRef = 8.toShort, countNonRef = 2.toShort))

  /**
    * implementation of pileup algorithm
    * @param alignments aligned reads
    * @return distributed collection of PileupRecords
    */
  def calculatePileup(alignments:RDD[SAMRecord]):RDD[PileupRecord] = {

    val contigLenMap = initContigLengths(alignments.first())
    collectEvents(alignments, contigLenMap)

    calculatePileupMock(alignments)
  }

  /**
    * Collects "interesting" (read start, stop, ref/nonref counting) events on alignments
    * @param alignments aligned reads
    * @param contigLenMap mapper between contig name and its length
    * @return distributed collection of PileupRecords
    */
  def collectEvents(alignments:RDD[SAMRecord], contigLenMap: Map[String, Int]): RDD[ContigEventAggregate] = {
    alignments.mapPartitions{partition =>
      val aggMap =  new mutable.HashMap[String, ContigEventAggregate]()
      val contigStartStopPartMap = new mutable.HashMap[String, Int]()
      val cigarMap = new mutable.HashMap[String, Int]()

      while (partition.hasNext) {
        val read = partition.next()
        val contig = DataQualityFuncs.cleanContig(read.getContig)

        // the first read from contig -> add new aggregate structure to map
        if (!aggMap.contains(contig)) {
          aggMap += contig -> initContigEventsAggregate(read, contigLenMap)
          contigStartStopPartMap += s"${contig}_start" -> read.getStart
          cigarMap += contig -> 0
        }
      }
      aggMap.toMap.values.iterator
    }

  }

  /**
    * initializes mapper between contig and its length basing on header values
    * @param read single aligned read (its header contains info about all contigs)
    * @return
    */
  def initContigLengths(read:SAMRecord): Map[String, Int] = {
    val contigLenMap = new mutable.HashMap[String, Int]()

    val sequenceList = read.getHeader.getSequenceDictionary.getSequences
    val sequenceSeq = JavaConverters.asScalaIteratorConverter(sequenceList.iterator()).asScala.toSeq

    for (sequence <- sequenceSeq) {
      logger.debug("sequence name {} => {} ", sequence.getSequenceName, sequence.getSequenceLength )
      val contigName = DataQualityFuncs.cleanContig(sequence.getSequenceName)
      contigLenMap += contigName -> sequence.getSequenceLength
    }
    contigLenMap.toMap
  }

  /**
    * initializes aggregate structure
    * @param read - single aligned read (first in contig)
    * @param contigLenMap - mapper between contig name and length
    * @return
    */
  def initContigEventsAggregate(read: SAMRecord, contigLenMap: Map[String, Int]): ContigEventAggregate ={
    val contig = DataQualityFuncs.cleanContig(read.getContig)
    val contigLen = contigLenMap(contig)
    val arrayLen = contigLen - read.getStart + 10
    ContigEventAggregate(contig, contigLen, new Array[Short](arrayLen), contigLen-1)
  }
}
