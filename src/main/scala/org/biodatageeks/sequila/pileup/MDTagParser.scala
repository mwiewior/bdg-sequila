package org.biodatageeks.sequila.pileup

import java.io.File

import htsjdk.samtools.reference.{FastaSequenceFile, IndexedFastaSequenceFile}
import htsjdk.samtools.{AlignmentBlock, BAMRecord, Cigar, CigarOperator, SAMRecord}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.biodatageeks.sequila.datasources.BAM.BDGAlignFileReaderWriter
import org.seqdoop.hadoop_bam.BAMBDGInputFormat

import scala.collection.mutable.ArrayBuffer
import collection.JavaConverters._
import util.control.Breaks._

case class MDOperator(length: Int, base: Char) //S means to skip n positions, not fix needed

object MDTagParser extends BDGAlignFileReaderWriter[BAMBDGInputFormat]{

  val logger: Logger = Logger.getLogger(this.getClass.getCanonicalName)
  final val fasta = new IndexedFastaSequenceFile(new File("/Users/marek/data/hs37d5.fa"))
//  val fasta = new IndexedFastaSequenceFile(new File("/Users/marek/data/Homo_sapiens_assembly18.fasta"))



  private def getReferenceFromRead(r: SAMRecord) ={
    logger.info(s"Processing read: ${r.getReadName}")
    logger.debug(s"MD: ${r.getAttribute("MD")}" )
    logger.debug(s"CIGAR: ${r.getCigarString}" )
    logger.debug(s"seq: ${r.getReadString}")
    logger.debug(s"contig ${r.getContig}")
    logger.debug(s"start ${r.getStart}")
    logger.debug(s"stop: ${r.getEnd}")

    val alignmentBlocks = r.getAlignmentBlocks
    var ind = 0
    logger.debug(s"Read has ${alignmentBlocks.size()} aligment blocks to process")
    if (alignmentBlocks !=  null  && alignmentBlocks.size() > 0){
      for(b <- alignmentBlocks.asScala){
        breakable {
          val deletions = {
            var alignBlockCnt = 0
            var delCnt = 0
            val cigar = r.getCigar.iterator()
            while(cigar.hasNext && alignBlockCnt <= ind){
              val el = cigar.next()
              if(el.getOperator == CigarOperator.MATCH_OR_MISMATCH)  alignBlockCnt += 1
              else if(el.getOperator == CigarOperator.DELETION) delCnt += el.getLength
            }
            delCnt
          }
          val contig = r.getContig
          val refStart = b.getReferenceStart - deletions
          val refEnd = b.getReferenceStart + b.getLength - 1
          if(r.getAttribute("MD") == null) {
            logger.error(s"MD tag for read ${r.getReadName} is missing. Skipping it.")
            break()
          }
          val reconstructedSeq = reconstructReference(r.getReadString, r.getCigar, r.getAttribute("MD").toString, ind)
          val isEqual = compareToRefSeq(reconstructedSeq, contig, refStart, refEnd)
          logger.debug(s"Comparing to ref ${isEqual.toString.toUpperCase}")
          if (!isEqual) throw new Exception("Bad ref seq reconstruction")
          ind += 1
        }
      }
    }



  }

  private def reconstructReference(seq : String, cigar: Cigar, mdTag: String, blockNum: Int = 0) = {
    val onlyDigits = "^[0-9]+$"
    val cigarElements = cigar.getCigarElements
    //fast if matches return seq, both MD and cigar is one block
    if(mdTag.matches(onlyDigits)
      && cigarElements.size() == 1 && cigarElements.get(0).getOperator ==  CigarOperator.MATCH_OR_MISMATCH ) {
      logger.debug(s"Seq aft: ${seq}")
      seq
    }

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
      var insertInd = 0
      while(ind <= blockNum){
        val cigarElement = cigarElements.get(cigarInd)
        val cigarOp = cigarElement.getOperator
        if(cigarOp == CigarOperator.MATCH_OR_MISMATCH ){
          ind += 1
        }
        if(cigarOp == CigarOperator.MATCH_OR_MISMATCH || cigarOp == CigarOperator.INSERTION || cigarOp == CigarOperator.SOFT_CLIP) {
          startPos += cigarElement.getLength
        }
        if(cigarOp == CigarOperator.INSERTION || cigarOp == CigarOperator.SOFT_CLIP) {
          insertInd += cigarElement.getLength
        }
        cigarInd += 1
      }
      if(ind > 0 ) {
        cigarInd = cigarInd - 1
        startPos = startPos - cigarElements.get(cigarInd).getLength
      }
      val blockLength =  cigarElements.get(cigarInd).getLength
      val seqFixed = applyMDTag(seq, mdOperators, startPos, blockLength, insertInd)
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
      if(m.last.isLetter && !m.contains('^') ){
        val skipPos = m.dropRight(1).toInt
        ab.append(MDOperator(skipPos, 'S') )
        ab.append(MDOperator(0, m.last.toUpper))
      }
      else if (m.last.isLetter && m.contains('^') ){ //encoding deletions as lowercase
        val arr =  m.split('^')
        val skipPos = arr.head.toInt
        ab.append(MDOperator(skipPos, 'S') )
        arr(1).foreach { b =>
          ab.append(MDOperator(0, b.toLower))
        }
      }
      else ab.append(MDOperator(m.toInt, 'S') )

    }
    ab.toIterator
  }

  private def applyMDTag(s: String,t: Iterator[MDOperator], pShift: Int = 0, blockLength: Int, inserts: Int = 0) = {
    logger.debug(s"Starting applying MD op at pos: ${pShift} with block length: ${blockLength}")
    val seqFixed = StringBuilder.newBuilder
    var ind = 0
    var isFirstOpInBlock = true
    while (t.hasNext) {
      val op = t.next()
      logger.debug(s"Applying MD op: ${op.toString}, ${ind}")
      if(ind < blockLength + pShift) {
        if(op.base == 'S' ) {
          logger.info(s"Index: ${ind}, inserts:  ${inserts}, blockLen:  ${blockLength}")
          val startPos =   { if(isFirstOpInBlock) pShift else 0 } + ind
          val shift  = {if (op.length - pShift + inserts >  blockLength) blockLength else  op.length - { if(isFirstOpInBlock) pShift - inserts else 0  }  }
          val endPos =  startPos + shift
          if(endPos > pShift){
            val seqToAppend = s.substring(  startPos , endPos )
            seqFixed.append(seqToAppend)
            logger.debug(s"Append seq length: ${seqToAppend.length} by skipping with ${seqToAppend}, start: ${startPos}, end: ${endPos}")
          }
          ind = endPos
          isFirstOpInBlock = false
        }
        else if(op.base != 'S' ){ //current block
          if(ind >= pShift){
            seqFixed.append(op.base.toUpper.toString)
            logger.debug(s"Append seq length: 1, at pos ${ind} with base ${op.base.toString}")
          }
          if (op.base.isUpper) ind += 1
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
    val sparkSession = SparkSession
      .builder()
      .master("local[1]")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("INFO")
    val sqlSession = sparkSession.sqlContext
    val bamRecords = readBAMFile(sqlSession,"/Users/marek/data/NA12878.chrom20.ILLUMINA.bwa.CEU.low_coverage.20121211.bam")
//    val bamRecords = readBAMFile(sqlSession,"/Users/marek/git/forks/bdg-sequila/src/test/resources/NA12878.slice.md.bam")



    sparkSession.time{
      val records = bamRecords
    //      .filter(_.getReadName=="SRR622461.74266492")
//          .filter(_.getReadName=="SRR622461.74266917")

        .map(getReferenceFromRead(_))
        .count()
    }




  }


}
