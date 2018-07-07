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

  def mergeArrays(a:Array[Short],b:Array[Short]) = {
    val c = new Array[Short](a.length)
    for(i<- 0 to c.length - 1 ){
      c(i) = (a(i) + b(i)).toShort
    }
    c
  }

  def readsToEventsArray(reads:RDD[SAMRecordWritable])   = {
    reads.mapPartitions{
      p =>
        val EDGE_BUFFER = 1000

        def eventOp(position:Int,contig:String,contigLength:Int,contigEventsMap:mutable.HashMap[String,Array[Short]],
                    contigEventsEdgesMap:mutable.HashMap[String,Array[Short]],incr:Boolean) ={

          if(position > EDGE_BUFFER && position < contigLength- EDGE_BUFFER){
            if(incr)
              contigEventsMap(contig)(position) = (contigEventsMap(contig)(position) + 1).toShort
            else
              contigEventsMap(contig)(position) = (contigEventsMap(contig)(position) - 1).toShort
          }
          else {
            if(incr)
              contigEventsEdgesMap(contig)(position) = (contigEventsEdgesMap(contig)(position) + 1).toShort
            else
              contigEventsEdgesMap(contig)(position) = (contigEventsEdgesMap(contig)(position) - 1).toShort

          }
        }

        val contigLengthMap = new mutable.HashMap[String, Int]()
        val contigEventsMap = new mutable.HashMap[String, Array[Short]]()
        val contigEventsEdgesMap = new mutable.HashMap[String, Array[Short]]()
        while(p.hasNext){
          val r = p.next()
          val read = r.get()
          val contig = read.getContig
          if(contig != null) {
            if (!contigLengthMap.contains(contig)) {
              val contigLength = read.getHeader.getSequence(contig).getSequenceLength
              contigLengthMap += contig -> contigLength
              contigEventsMap += contig -> new Array[Short](contigLength - 2*EDGE_BUFFER)
              contigEventsEdgesMap += contig -> new Array[Short](2 * EDGE_BUFFER)
            }
            val cigarIterator = read.getCigar.iterator()
            var position = read.getStart
            var contigLength = contigLengthMap(contig)
            while(cigarIterator.hasNext){
                val cigarElement = cigarIterator.next()
                val cigarOpLength = cigarElement.getLength
                val cigarOp = cigarElement.getOperator
                if (cigarOp == CigarOperator.M || cigarOp == CigarOperator.X || cigarOp == CigarOperator.EQ) {


                  eventOp(position,contig,contigLength,contigEventsMap,contigEventsEdgesMap,true)
                  position += cigarOpLength
                  eventOp(position,contig,contigLength,contigEventsMap,contigEventsEdgesMap,false)

                }
                else  if (cigarOp == CigarOperator.N || cigarOp == CigarOperator.D) position += cigarOpLength
           }

          }
      }
        contigEventsMap.iterator
    }//.reduceByKey((a,b)=> mergeArrays(a,b) )
  }

  def eventsToCoverage(sampleId:String,events: RDD[(String,Array[Short])]) = {
    events.mapPartitions{
      p => p.map(r=>{
        val contig = r._1
        //val result = new Array[CoverageRecord](r._2.filter(_>0).length)

        val covArray = new Array[Short](r._2.length)
        var pos = 0
        var cov = 0
        var ind = 0
        var resultLength = 0
        val covArrayLength = r._2.length
        while(ind< covArrayLength){
          cov += r._2(ind)
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
            result(ind) = CovRecord(contig, pos, covArray(i))
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
      .master("local[4]")
      .config("spark.driver.memory","4g")
      .getOrCreate()

    lazy val alignments = spark.sparkContext
      .newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat]("/Users/marek/data/NA12878.chrom20.ILLUMINA.bwa.CEU.low_coverage.20121211.bam")

    lazy val events = readsToEventsArray(alignments.map(r=>r._2))
    spark.time{
      //println(alignments.count)
    }
    spark.time{

     println(events.count)

    }
    lazy val coverage = eventsToCoverage("test",events)
    spark.time{

      println(coverage.count())

    }
    spark.stop()


  }

}
