package org.biodatageeks.preprocessing.coverage

import java.io.File

import htsjdk.samtools.{Cigar, CigarOperator}
import org.apache.hadoop.io.LongWritable
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.hammerlab.paths.Path
import org.seqdoop.hadoop_bam.{BAMInputFormat, SAMRecordWritable}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class CovRecord(contig:String,pos:Int, cov:Short)
object CoverageMOS {

  def mergeArrays(a:(Array[Short],Int,Int,Int),b:(Array[Short],Int,Int,Int)) = {
    val c = new Array[Short](a._4)
    for(i<- 0 to c.length - 1 ){
      c(i) = (( if(i >= a._2 && i < a._2+a._1.length) a._1(i-a._2) else 0)  + (if(i >= b._2 && i < b._2+b._1.length) b._1(i-b._2) else 0)).toShort
    }
    (c,0,a._4,a._4)
  }

  def readsToEventsArray(reads:RDD[SAMRecordWritable])   = {
    reads.mapPartitions{
      p =>
//        val EDGE_BUFFER = 1000

        def eventOp(pos:Int,startPart:Int,contig:String,contigEventsMap:mutable.HashMap[String,(Array[Short],Int,Int,Int)],incr:Boolean) ={

            val position = pos - startPart
//          if(position > EDGE_BUFFER && position < (contigLength - EDGE_BUFFER) ){
            if(incr)
              contigEventsMap(contig)._1(position) = (contigEventsMap(contig)._1(position) + 1).toShort
            else
              contigEventsMap(contig)._1(position) = (contigEventsMap(contig)._1(position) - 1).toShort
//          }
//          else {
//            if(incr)
//              contigEventsMap(s"${contig}_toreduce")(position) = (contigEventsMap(s"${contig}_toreduce")(position) + 1).toShort
//            else
//              contigEventsMap(s"${contig}_toreduce")(position) = (contigEventsMap(s"${contig}_toreduce")(position) - 1).toShort
        }

        val contigLengthMap = new mutable.HashMap[String, Int]()
        val contigEventsMap = new mutable.HashMap[String, (Array[Short],Int,Int,Int)]()
        val contigStartStopPartMap = new mutable.HashMap[(String),Int]()
        var lastContig: String = null
        var lastPosition = 0
        while(p.hasNext){
          val r = p.next()
          val read = r.get()
          val contig = read.getContig
          if(contig != null && read.getFlags!= 1796) {
            if (!contigLengthMap.contains(contig)) {
              val contigLength = read.getHeader.getSequence(contig).getSequenceLength
              contigLengthMap += contig -> contigLength
              contigEventsMap += contig -> (new Array[Short](contigLength-read.getStart), read.getStart,contigLength-1, contigLength)
              contigStartStopPartMap += s"${contig}_start" -> read.getStart
            }
            val cigarIterator = read.getCigar.iterator()
            var position = read.getStart
            val contigLength = contigLengthMap(contig)
            while(cigarIterator.hasNext){
                val cigarElement = cigarIterator.next()
                val cigarOpLength = cigarElement.getLength
                val cigarOp = cigarElement.getOperator
                if (cigarOp == CigarOperator.M || cigarOp == CigarOperator.X || cigarOp == CigarOperator.EQ) {
                  eventOp(position,contigStartStopPartMap(s"${contig}_start"),contig,contigEventsMap,true)
                  position += cigarOpLength
                  eventOp(position,contigStartStopPartMap(s"${contig}_start"),contig,contigEventsMap,false)
                }
                else  if (cigarOp == CigarOperator.N || cigarOp == CigarOperator.D) position += cigarOpLength
           }

          }

      }
        contigEventsMap.mapValues(r=>
        {
          var maxIndex = 0
          var i = 0
          while(i < r._1.length ){
            if(r._1(i) != 0) maxIndex = i
            i +=1
          }
          (r._1.slice(0,maxIndex+1),r._2,r._3,r._4)
        }).iterator
    }.reduceByKey((a,b)=> mergeArrays(a,b))
  }

  def eventsToCoverage(sampleId:String,events: RDD[(String,(Array[Short],Int,Int,Int))]) = {
    events.mapPartitions{
      p => p.map(r=>{
        val contig = r._1
        val covArrayLength = r._2._1.length
        val covArray = new Array[Short](covArrayLength)
        var pos = 0
        var cov = 0
        var ind = 0
        var resultLength = 0

        while(ind< covArrayLength){
          cov += r._2._1(ind)
          val currPos = pos
          pos += 1
          covArray(ind) = cov.toShort
          if(cov > 0) resultLength += 1
          ind +=1
        }

        val result = new Array[CovRecord](resultLength)
        ind = 0
        var i = 0
        while(i < covArrayLength){
          if(covArray(i) >0) {
            result(ind) = CovRecord(contig, i, covArray(i))
            ind += 1
          }
          i+= 1
        }
      result.iterator
      })
    }.flatMap(r=>r)
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .config("spark.driver.memory","6g")
      .getOrCreate()

    lazy val alignments = spark.sparkContext
      .newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat]("/Users/marek/data/NA12878.chrom20.ILLUMINA.bwa.CEU.low_coverage.20121211.bam")

    lazy val events = readsToEventsArray(alignments.map(r=>r._2))
    spark.time{
      //println(alignments.count)
    }
    spark.time{

    //println(events.count)

    }

    //lazy val combinedRes = combineEvents(events,63025520,1000)
    lazy val coverage = eventsToCoverage("test",events)
    spark.time{
      println(coverage.count)

    }
    spark.stop()


  }

}
