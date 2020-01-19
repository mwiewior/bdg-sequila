package org.biodatageeks.sequila.pileup

import java.io.File

import htsjdk.samtools.reference.{FastaSequenceFile, IndexedFastaSequenceFile}
import htsjdk.samtools.{AlignmentBlock, BAMRecord, Cigar, CigarOperator, SAMRecord}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.biodatageeks.sequila.datasources.BAM.BDGAlignFileReaderWriter
import org.biodatageeks.sequila.pileup.MDTagParser.compareToRefSeq
import org.seqdoop.hadoop_bam.BAMBDGInputFormat

import scala.collection.mutable.ArrayBuffer
import collection.JavaConverters._

case class MDOperator(length: Int, base: Char) //S means to skip n positions, not fix needed

object MDTagParser extends BDGAlignFileReaderWriter[BAMBDGInputFormat]{

  val logger: Logger = Logger.getLogger(this.getClass.getCanonicalName)

  val sparkSession = SparkSession
      .builder()
      .master("local[1]")
      .getOrCreate()
  val sqlSession = sparkSession.sqlContext
  val bamRecords = readBAMFile(sqlSession,"/Users/marek/git/forks/bdg-sequila/src/test/resources/NA12878.slice.md.bam")

  val fasta = new IndexedFastaSequenceFile(new File("/Users/marek/data/Homo_sapiens_assembly18.fasta"))

  private def getReferenceFromRead(r: SAMRecord) ={
    logger.debug(s"Processing read: ${r.getReadName}")
    logger.debug(s"MD: ${r.getAttribute("MD")}" )
    logger.debug(s"CIGAR: ${r.getCigarString}" )
    logger.debug(s"seq: ${r.getReadString}")
    logger.debug(s"contig ${r.getContig}")
    logger.debug(s"start ${r.getStart}")
    logger.debug(s"stop: ${r.getEnd}")

    val alignmentBlocks = r.getAlignmentBlocks
    var ind = 0
    logger.debug(s"Read has ${alignmentBlocks.size()} aligment blocks to process")
    for(b <- alignmentBlocks.asScala){
//      val b = alignmentBlocks.next()
      val contig = r.getContig
      val refStart = b.getReferenceStart
      val refEnd = b.getReferenceStart + b.getLength - 1
      val reconstructedSeq = reconstructReference(r.getReadString, r.getCigar, r.getAttribute("MD").toString, ind)
      val isEqual = compareToRefSeq(reconstructedSeq, contig, refStart, refEnd)
      logger.debug(s"Comparing to ref ${isEqual.toString.toUpperCase}")
      if (!isEqual) throw new Exception("Bad ref seq reconstruction")
      ind += 1
    }


  }

  private def reconstructReference(seq : String, cigar: Cigar, mdTag: String, blockNum: Int = 0) = {
    val onlyDigits = "^[0-9]+$"
    val cigarElements = cigar.getCigarElements
    //fast if matches return seq, both MD and cigar is one block
    if(mdTag.matches(onlyDigits)
      && cigarElements.size() == 1 && cigarElements.get(0).getOperator ==  CigarOperator.MATCH_OR_MISMATCH )
      seq
    //fixing SNPs, cigar is one block size
    else if (cigarElements.size() == 1 && cigarElements.get(0).getOperator ==  CigarOperator.MATCH_OR_MISMATCH) {

      val mdOperators = parseMDTag(mdTag)
      val seqFixed = applyMDTag(seq,mdOperators, 0, cigarElements.get(0).getLength)
      logger.debug(s"Seq aft: ${seqFixed}")
      seqFixed
    }
    //handling INDELS, multiple aligment blocks
    else {
      logger.debug("Processing complex CIGAR with INDELs")
      logger.debug(s"Processing ${blockNum}(+1) alignment block of ${cigar.toString}")
      val mdOperators = parseMDTag(mdTag) //FIXME: now it recalculates MD for each block can be done once per read !!!
      var startPos = 0
      val cigarElements = cigar.getCigarElements
      var ind = 0
      var cigarInd = 0
      while(ind <= blockNum &&  blockNum > 0){
        val cigarElement = cigarElements.get(cigarInd)
        val cigarOp = cigarElement.getOperator
        if(cigarOp == CigarOperator.MATCH_OR_MISMATCH ){
          ind += 1
        }
        if(cigarOp == CigarOperator.MATCH_OR_MISMATCH || cigarOp == CigarOperator.INSERTION) {
          startPos += cigarElement.getLength
        }
        cigarInd += 1
      }
      if(ind > 0 ) {
        cigarInd = cigarInd - 1
        startPos = startPos - cigarElements.get(cigarInd).getLength
      }
      val blockLength =  cigarElements.get(cigarInd).getLength
      val seqFixed = applyMDTag(seq, mdOperators, startPos, blockLength)
      logger.debug(s"Seq aft: ${seqFixed}")
      seqFixed
    }
  }

  private def parseMDTag(t : String) = {
    logger.debug(s"Parsing MD tag: ${t}")
    val pattern = "([0-9]+)\\^?([A-Za-z]+)?".r
    val ab = new ArrayBuffer[MDOperator]()
    val matches = pattern
      .findAllIn(t)
    while (matches.hasNext) {
      val m = matches.next().toUpperCase
      logger.debug(s"MD operator: ${m}")
      if(m.last.isLetter){
        val skipPos = m.dropRight(1).toInt
        ab.append(MDOperator(skipPos, 'S') )
        ab.append(MDOperator(0, m.last))
      }
      else ab.append(MDOperator(m.toInt, 'S') )

    }
    ab.toIterator
  }

  private def applyMDTag(s: String,t: Iterator[MDOperator], pShift: Int = 0, blockLength: Int) = {
    logger.debug(s"Starting applying MD op at pos: ${pShift} with block length: ${blockLength}")
    val seqFixed = StringBuilder.newBuilder
    var ind = 0
    while (t.hasNext) {
      val op = t.next()
      if(ind <= blockLength) {
        logger.debug(s"Applying MD op: ${op.toString}")
        if(op.base == 'S' ) {
          val seqToAppend = s.substring(  if(ind <= pShift)  pShift + ind else ind , ind + math.min(op.length, blockLength-ind) )
          seqFixed.append(seqToAppend)
          if(ind <= pShift) ind += op.length - pShift
          else ind += op.length
        }
        else if(op.base != 'S' && ind >= pShift){
          seqFixed.append(op.base.toString)
          ind += 1
        }
      }
      else ind += op.length
    }
    seqFixed.toString()
  }
  private def compareToRefSeq(seq:String, contig: String, start : Int, end : Int ) = {

    val refSeq = fasta.getSubsequenceAt( contig, start.toLong, end.toLong)
    logger.debug(s"Seq ref: ${refSeq.getBaseString.toUpperCase}")
    seq.equalsIgnoreCase(refSeq.getBaseString)

  }

  def main(args: Array[String]): Unit = {

    val records = bamRecords
      .collect()
//      .filter(r => r.getAttribute("MD") == "71a0a1c0")
      .iterator
    while (records.hasNext){
      val r = records.next()
      getReferenceFromRead(r)
    }




  }


}
