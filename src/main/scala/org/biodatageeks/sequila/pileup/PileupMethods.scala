package org.biodatageeks.sequila.pileup

import htsjdk.samtools.{CigarOperator, SAMRecord}
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD
import org.biodatageeks.sequila.utils.DataQualityFuncs
import scala.collection.{JavaConverters, mutable}


/**
  * Class implementing pileup calculations on set of aligned reads
  */
object PileupMethods {

  val logger = LoggerFactory.getLogger(this.getClass.getCanonicalName)

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


    val output = collectEvents(alignments)
    logger.debug("Events count: {}", output.count())

    calculatePileupMock(alignments)
  }

  /**
    * Collects "interesting" (read start, stop, ref/nonref counting) events on alignments
    *
    * @param alignments aligned reads
    * @return distributed collection of PileupRecords
    */
  def collectEvents(alignments:RDD[SAMRecord]): RDD[ContigEventAggregate] = {
    val contigLenMap = initContigLengths(alignments.first())

    alignments.mapPartitions{partition =>
      val aggMap =  new mutable.HashMap[String, ContigEventAggregate]()
      val contigMaxReadLen = new mutable.HashMap[String, Int]()

      while (partition.hasNext) {
        val read = partition.next()
        val contig = DataQualityFuncs.cleanContig(read.getContig)

        if (!aggMap.contains(contig))
          handleFirstReadForContigInPartition(read, contig, contigLenMap, contigMaxReadLen, aggMap)

        val contigEventAggregate = aggMap(contig)
        analyzeRead (read, contig, contigEventAggregate, contigMaxReadLen)
      }
      lazy val output = prepareOutputAggregates (aggMap, contigMaxReadLen)
      output.toMap.values.iterator
    }

  }

  private def handleFirstReadForContigInPartition(read: SAMRecord, contig: String, contigLenMap: Map[String, Int], contigMaxReadLen: mutable.HashMap[String, Int], aggMap: mutable.HashMap[String, ContigEventAggregate]) = {
    val contigLen = contigLenMap(contig)
    val arrayLen = contigLen - read.getStart + 10

    val contigEventAggregate = ContigEventAggregate (
      contig = contig,
      contigLen = contigLen,
      cov = new Array[Short](arrayLen),
      startPosition = read.getStart,
      maxPosition = contigLen - 1 )

    aggMap += contig -> contigEventAggregate
    contigMaxReadLen += contig -> 0
  }

  /**
    * simply updates the map between contig and max read length (for overlaps). If current read len is greater update map
    *
    * @param read analyzed aligned read from partition
    * @param contig read contig (cleaned)
    * @param contigMaxReadLen map between contig and max read length
    */
  @inline
  private def updateMaxReadLenInContig(read: SAMRecord, contig: String, contigMaxReadLen: mutable.HashMap[String, Int]): Unit = {
    val seqLen = read.getReadLength
    if (seqLen > contigMaxReadLen(contig))
      contigMaxReadLen(contig) = seqLen
  }

  /**
    * updates events array for contig and updates contig's max read length
    * @param read analyzed aligned read from partition
    * @param contig read contig (cleaned)
    * @param eventAggregate object holding current state of events aggregate in this contig
    * @param contigMaxReadLen map between contig and max read length (for overlaps)
    */
  def analyzeRead(read: SAMRecord, contig:String, eventAggregate: ContigEventAggregate, contigMaxReadLen: mutable.HashMap[String, Int]): Unit = {

    val partitionStart=eventAggregate.startPosition
    var position = read.getStart
    val cigarIterator = read.getCigar.iterator()

    while (cigarIterator.hasNext) {
      val cigarElement = cigarIterator.next()
      val cigarOpLength = cigarElement.getLength
      val cigarOp = cigarElement.getOperator

      // update events array according to read alignment blocks start/end
      if (cigarOp == CigarOperator.M || cigarOp == CigarOperator.X || cigarOp == CigarOperator.EQ) {
        updateContigEventsArray(position, partitionStart, contig, eventAggregate, delta = 1)
        position += cigarOpLength
        updateContigEventsArray(position, partitionStart, contig, eventAggregate, delta = -1)
      }
      else if (cigarOp == CigarOperator.N || cigarOp == CigarOperator.D)
        position += cigarOpLength
    }

    // seq len is not equal to cigar len (typically longer, because of clips, but the value is ready to use, doesn't nedd to be computed)
    updateMaxReadLenInContig(read, contig, contigMaxReadLen)
  }

  /**
    * transforms map structure of contigEventAggregates, by reducing number of last zeroes in the cov array
    * also adds calculated maxCigar len to output
    * @param aggMap mapper between contig and contigEventAggregate
    * @param cigarMap mapper between contig and max length of cigar in given
    * @return
    */
  def prepareOutputAggregates(aggMap: mutable.HashMap[String, ContigEventAggregate], cigarMap: mutable.HashMap[String, Int] ): mutable.HashMap[String, ContigEventAggregate] = {
    aggMap.map(r => {
      val contig = r._1
      val contigEventAgg = r._2

      val maxIndex: Int = contigEventAgg.cov.lastIndexWhere(x => x != 0)
      (contig, ContigEventAggregate(
        contig,
        contigEventAgg.contigLen,
        contigEventAgg.cov.slice(0, maxIndex + 1),
        contigEventAgg.startPosition,
        contigEventAgg.startPosition + maxIndex,
        cigarMap(contig)))

    })
  }

  /**
    * updates events array for contig. Should be invoked with delta = 1 for alignment start and -1 for alignment stop
    * @param pos position to be changed
    * @param startPart starting position of partition (offset)
    * @param contig - contig
    * @param eventAggregate - aggregate
    * @param delta - value to be added to position
    */

  @inline
  def updateContigEventsArray(
                       pos: Int,
                       startPart: Int,
                       contig: String,
                       eventAggregate: ContigEventAggregate,
                       delta: Short): Unit = {

    val position = pos - startPart
    eventAggregate.cov(position) = (eventAggregate.cov(position) + delta).toShort
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

}
