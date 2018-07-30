package org.biodatageeks.preprocessing.coverage

import java.io.File
import java.sql.DriverManager

import htsjdk.samtools.{BAMFileReader, CigarOperator, SamReaderFactory, ValidationStringency}
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.seqdoop.hadoop_bam.{AnySAMInputFormat, BAMBDGInputFormat, BAMInputFormat, SAMRecordWritable}
import org.seqdoop.hadoop_bam.util.SAMHeaderReader

import scala.collection.mutable
import htsjdk.samtools._
//import com.intel.gkl.compression._

import htsjdk.samtools.util.zip.InflaterFactory
import java.util.zip.Inflater

import scala.collection.mutable

import org.apache.ignite.{Ignite, IgniteCache, Ignition}

case class CovRecord(contigName:String,start:Int,end:Int, cov:Short)

object CoverageMethodsMos {

  def mergeArrays(a:(Array[Short],Int,Int,Int),b:(Array[Short],Int,Int,Int)) = {
    //val c = new Array[Short](a._4)
    val c = new Array[Short](math.min(math.abs(a._2 - b._2) + math.max(a._1.length, b._1.length), a._4))
    val lowerBound = math.min(a._2, b._2)

    var i = lowerBound
    while (i <= c.length + lowerBound - 1) {
      c(i - lowerBound) = ((if (i >= a._2 && i < a._2 + a._1.length) a._1(i - a._2) else 0) + (if (i >= b._2 && i < b._2 + b._1.length) b._1(i - b._2) else 0)).toShort
      i += 1
    }
    //    for(i<- lowerBound to c.length + lowerBound - 1 ){
    //      c(i-lowerBound) = (( if(i >= a._2 && i < a._2+a._1.length) a._1(i-a._2) else 0)  + (if(i >= b._2 && i < b._2+b._1.length) b._1(i-b._2) else 0)).toShort
    //    }

    (c, lowerBound, lowerBound + c.length, a._4)
  }

  @inline def eventOp(pos:Int, startPart:Int, contig:String, contigEventsMap:mutable.HashMap[String,(Array[Short],Int,Int,Int)], incr:Boolean) ={

    val position = pos - startPart
    if(incr)
      contigEventsMap(contig)._1(position) = (contigEventsMap(contig)._1(position) + 1).toShort
    else
      contigEventsMap(contig)._1(position) = (contigEventsMap(contig)._1(position) - 1).toShort
  }

  def readsToEventsArray(reads:RDD[SAMRecordWritable])   = {
    reads.mapPartitions{
      p =>
        val contigLengthMap = new mutable.HashMap[String, Int]()
        val contigEventsMap = new mutable.HashMap[String, (Array[Short],Int,Int,Int)]()
        val contigStartStopPartMap = new mutable.HashMap[(String),Int]()
        val cigarMap = new mutable.HashMap[String, Int]()
        //var maxCigarLength = 0
        while(p.hasNext){
          val r = p.next()
          val read = r.get()
          val contig = read.getContig
          if(contig != null && read.getFlags!= 1796) {
            if (!contigLengthMap.contains(contig)) { //FIXME: preallocate basing on header, n
              val contigLength = read.getHeader.getSequence(contig).getSequenceLength
              //println(s"${contig}:${contigLength}:${read.getStart}")
              contigLengthMap += contig -> contigLength
              contigEventsMap += contig -> (new Array[Short](contigLength-read.getStart+10), read.getStart,contigLength-1, contigLength)
              contigStartStopPartMap += s"${contig}_start" -> read.getStart
              cigarMap += contig -> 0
            }
            val cigarIterator = read.getCigar.iterator()
            var position = read.getStart
            //val contigLength = contigLengthMap(contig)
            var currCigarLength = 0
            while(cigarIterator.hasNext){
              val cigarElement = cigarIterator.next()
              val cigarOpLength = cigarElement.getLength
              currCigarLength += cigarOpLength
              val cigarOp = cigarElement.getOperator
              if (cigarOp == CigarOperator.M || cigarOp == CigarOperator.X || cigarOp == CigarOperator.EQ) {
                eventOp(position,contigStartStopPartMap(s"${contig}_start"),contig,contigEventsMap,true) //Fixme: use variable insteaad of lookup to a map
                position += cigarOpLength
                eventOp(position,contigStartStopPartMap(s"${contig}_start"),contig,contigEventsMap,false)
              }
              else  if (cigarOp == CigarOperator.N || cigarOp == CigarOperator.D) position += cigarOpLength
            }
            if(currCigarLength > cigarMap(contig)) cigarMap(contig) = currCigarLength

          }
        }
        println(s" max cigar length: ${cigarMap.toString()}")
        lazy val output = contigEventsMap
          .map(r=>
          {
            var maxIndex = 0
            var i = 0
            while(i < r._2._1.length ){
              if(r._2._1(i) != 0) maxIndex = i
              i +=1
            }
            (r._1,(r._2._1.slice(0,maxIndex+1),r._2._2,r._2._2+maxIndex,r._2._4,s"${r._1}_${r._2._2+maxIndex}") )//
          }
          )
        output
          .foreach{
            r=>
              {
                persistPartEdges(r._1,cigarMap(r._1),r._2)
              }
          }
        output.iterator
    }

  }

  private def persistPartEdges(contigName:String, maxCigarLength:Int, cov: ((Array[Short],Int,Int,Int,String)) ) = {
    Class.forName("org.apache.ignite.IgniteJdbcThinDriver")
    val conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")
    val batchSize = 1000
    val startPos = cov._3 - maxCigarLength + 1
    val covArray = cov._1.takeRight(maxCigarLength)
    val partKey = cov._5
    val sqlInsert =
      """
        |INSERT INTO coverage(contig_name,pos,cov,part_key)
        |VALUES (?,?,?,?)
      """.stripMargin
    val ps = conn.prepareStatement(sqlInsert)
    var i = 0
    while(i < covArray.length){

      ps.setString(1,contigName)
      ps.setInt(2, startPos + i)
      ps.setInt(3,covArray(i))
      ps.setString(4,partKey)

      ps.addBatch()
      i+=1
      if(i % batchSize == 0)
        ps.executeBatch()

    }
    ps.executeBatch()




  }

  def mergePartEdge(c:(String,(Array[Short],Int,Int,Int,String)) ) ={
    Class.forName("org.apache.ignite.IgniteJdbcThinDriver")
    val conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")
    val stmt = conn.createStatement()
    val contigName = c._1
    val minPos = c._2._2
    val maxPos = c._2._3
    val contigLength = c._2._4
    val partKey = c._2._5
    val cov = c._2._1
    val query =
      s"""
        |SELECT pos, cov
        |FROM coverage
        |WHERE contig_name='${contigName}'
        |AND pos>=${minPos} AND pos<=${maxPos}
        |AND part_key <>'${partKey}'
      """.stripMargin

      val rs = stmt.executeQuery(query)
      var cnt = 0
    //add overlaping reads from other partitions
      while(rs.next()){
        cov(rs.getInt("pos") - minPos) = ( cov(rs.getInt("pos") - minPos) + rs.getInt("cov")).toShort
        cnt += 1
      }
      println(s"${partKey} updated ${cnt} positions")

      //delete rows that were already fetched and added
      val deleteStmt =
        s"""
          |DELETE FROM coverage
          |WHERE
          |part_key<>'${partKey}'
          |AND contig_name='${contigName}'
          |AND
          |pos>=${minPos} AND pos<=${maxPos}
        """.stripMargin

        stmt.executeUpdate(deleteStmt)
        //conn.commit()
    val query2 =
        s"""
           |SELECT max(pos) as maxpos
           |FROM coverage
           |WHERE contig_name='${contigName}'
           |AND pos>=${minPos} AND pos<=${maxPos}
           |AND part_key ='${partKey}'
    """.stripMargin
    var maxRemainPos = Int.MaxValue
    val rs2 = stmt.executeQuery(query2)
    while(maxRemainPos < maxPos) {
      while (rs2.next()) {
        maxRemainPos = rs2.getInt("maxpos")
      }
      if(maxRemainPos < maxPos) Thread.sleep(100)
    }
    println(s"${partKey} maxPos after merge ${maxRemainPos}")
    (contigName,(cov.take(maxRemainPos-minPos),minPos,maxRemainPos,contigLength))


  }
//  def shrinkPart(c:(String,(Array[Short],Int,Int,Int,String)) ) ={
//    Class.forName("org.apache.ignite.IgniteJdbcThinDriver")
//    val conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")
//    val stmt = conn.createStatement()
//    val contigName = c._1
//    val minPos = c._2._2
//    val maxPos = c._2._3
//    val contigLength = c._2._4
//    val partKey = c._2._5
//    val cov = c._2._1
//    val query =
//      s"""
//         |SELECT max(pos) as maxpos
//         |FROM coverage
//         |WHERE contig_name='${contigName}'
//         |AND pos>=${minPos} AND pos<=${maxPos}
//         |AND part_key ='${partKey}'
//      """.stripMargin
//    var maxRemainPos = 0
//    val rs = stmt.executeQuery(query)
//    while(rs.next()){
//      maxRemainPos = rs.getInt("maxpos")
//    }
//    println(s"${partKey} maxPos after merge ${maxRemainPos}")
//    (contigName,(cov.take(maxRemainPos-minPos),minPos,maxRemainPos,contigLength))
//
//  }
  def reduceEventsArray(covEvents: RDD[(String,(Array[Short],Int,Int,Int))]) =  {
    //a:(Array[Short],Int,Int,Int) => (covEvents,startPos,maxPos,contigLength)
    covEvents.reduceByKey((a,b)=> mergeArrays(a,b))
  }




  def eventsToCoverage(sampleId:String,events: RDD[(String,(Array[Short],Int,Int,Int))]) = {
    events
      .mapPartitions{ p => p.map(r=>{
        val contig = r._1
        val covArrayLength = r._2._1.length
        var cov = 0
        var ind = 0
        val posShift = r._2._2

        val result = new Array[CovRecord](covArrayLength)
        var i = 0
        var prevCov = 0
        var blockLength = 0
        while(i < covArrayLength){
          cov += r._2._1(i)
          if(cov >0) {
            if(prevCov>0 && prevCov != cov) {
              result(ind) = CovRecord(contig,i+posShift - blockLength, i + posShift-1, prevCov.toShort)
              blockLength = 0
              ind += 1
            }
            blockLength +=1
            prevCov = cov
          }
          i+= 1
        }
        result.take(ind).iterator
      })
    }.flatMap(r=>r)
  }


  def initInMemoryGrid() ={
    Class.forName("org.apache.ignite.IgniteJdbcThinDriver")
    val conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")
    val stmt = conn.createStatement()

    val ddlCreate =
      """
        |CREATE TABLE IF NOT EXISTS coverage (
        |contig_name VARCHAR,
        |pos INT,
        |cov SMALLINT,
        |part_key VARCHAR,
        |PRIMARY KEY (contig_name, pos)
        |)
      """.stripMargin
  val ddlDrop =
    """
      |DROP TABLE IF EXISTS coverage
    """.stripMargin
    stmt.executeUpdate(ddlDrop)
    stmt.executeUpdate(ddlCreate)

  }



}
