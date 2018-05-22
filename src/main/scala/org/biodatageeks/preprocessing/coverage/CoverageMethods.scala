package org.biodatageeks.preprocessing.coverage

import htsjdk.samtools.{Cigar, CigarOperator, SAMUtils, TextCigarCodec}

import org.apache.spark.rdd.RDD

import org.apache.spark.storage.StorageLevel
import org.biodatageeks.datasources.BAM.BAMRecord

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


case class CoverageRecord(sampleId:String,
                              chr:String,
                              position:Int,
                              coverage:Int)

case class PartitionCoverage(covMap: mutable.HashMap[(String, Int), Array[Int]],
                             maxCigarLength: Int,
                             outputSize: Int,
                             chrMinMax: Array[(String,Int)] )



class CoverageReadFunctions(covReadRDD:RDD[BAMRecord]) extends Serializable {

  def baseCoverage(minMapq: Option[Int], numTasks: Option[Int] = None, sorted: Boolean):RDD[CoverageRecord] ={
    val sampleId = covReadRDD
      .first()
      .sampleId
    lazy val cov =numTasks match {
        case Some(n) => covReadRDD.repartition(n)
        case _ => covReadRDD
      }

      lazy val covQual = minMapq match {
        case Some(qual) => cov //FIXME add filtering
        case _ => cov
      }
        lazy val partCov = {
          sorted match {
            case true => covQual//.instrument()
            case _ => covQual.sortBy(r => (r.contigName, r.start))
          }
        }.mapPartitions { partIterator =>
          val covMap = new mutable.HashMap[(String, Int), Array[Int]]()
          val numSubArrays = 10000
          val subArraySize = 250000000 / numSubArrays
          val chrMinMax = new ArrayBuffer[(String, Int)]()
          var maxCigarLength = 0
          var lastChr = "NA"
          var lastPosition = 0
          var outputSize = 0

          for (cr <- partIterator) {
            val cigar = TextCigarCodec.decode(cr.cigar)
            val cigIterator = cigar.iterator()
            var position = cr.start
            val cigarStartPosition = position
            while (cigIterator.hasNext) {
              val cigarElement = cigIterator.next()
              val cigarOpLength = cigarElement.getLength
              val cigarOp = cigarElement.getOperator
              if (cigarOp == CigarOperator.M || cigarOp == CigarOperator.X || cigarOp == CigarOperator.EQ) {
                var currPosition = 0
                while (currPosition < cigarOpLength) {
                  val subIndex = position % numSubArrays
                  val index = position / numSubArrays

                  if (!covMap.keySet.contains(cr.contigName, index)) {
                    covMap += (cr.contigName, index) -> Array.fill[Int](subArraySize)(0)
                  }
                  covMap(cr.contigName, index)(subIndex) += 1
                  position += 1
                  currPosition += 1

                  /*add first*/
                  if (outputSize == 0) chrMinMax.append((cr.contigName, position))
                  if (covMap(cr.contigName, index)(subIndex) == 1) outputSize += 1

                }
              }
              else if (cigarOp == CigarOperator.N || cigarOp == CigarOperator.D) position += cigarOpLength
            }
            val currLength = position - cigarStartPosition
            if (maxCigarLength < currLength) maxCigarLength = currLength
            lastPosition = position
            lastChr = cr.contigName
          }
          chrMinMax.append((lastChr, lastPosition))
          Array(PartitionCoverage(covMap, maxCigarLength, outputSize, chrMinMax.toArray)).iterator
        }.persist(StorageLevel.MEMORY_AND_DISK_SER)
        val maxCigarLengthGlobal = partCov.map(r => r.maxCigarLength).reduce((a, b) => scala.math.max(a, b))
        lazy val combOutput = partCov.mapPartitions { partIterator =>
          /*split for reduction basing on position and max cigar length across all partitions - for gap alignments*/
          val partitionCoverageArray = (partIterator.toArray)
          val partitionCoverage = partitionCoverageArray(0)
          val chrMinMax = partitionCoverage.chrMinMax
          lazy val output = new Array[Array[CoverageRecord]](2)
          lazy val outputArray = new Array[CoverageRecord](partitionCoverage.outputSize)
          lazy val outputArraytoReduce = new ArrayBuffer[CoverageRecord]()
          val covMap = partitionCoverage.covMap
          var cnt = 0
          for (key <- covMap.keys) {
            var locCnt = 0
            for (value <- covMap.get(key).get) {
              if (value > 0) {
                val position = key._2 * 10000 + locCnt
                if (key._1 == chrMinMax.head._1 && position <= chrMinMax.head._2 + maxCigarLengthGlobal ||
                  key._1 == chrMinMax.last._1 && position >= chrMinMax.last._2 - maxCigarLengthGlobal)
                  outputArraytoReduce.append(CoverageRecord(sampleId,key._1, position, value))
                else
                  outputArray(cnt) = (CoverageRecord(sampleId,key._1, position, value))
                cnt += 1
              }
              locCnt += 1
            }
          } /*only records from the beginning and end of the partition for reduction the rest pass-through */
          output(0) = outputArray.filter(r => r != null)
          output(1) = outputArraytoReduce.toArray
          Iterator(output)
        }
        //partCov.unpersist()
        lazy val covReduced = combOutput.flatMap(r => r.array(1)).map(r => ((r.chr, r.position), r))
          .reduceByKey((a, b) => CoverageRecord(sampleId,a.chr, a.position, a.coverage + b.coverage))
          .map(_._2)
        partCov.unpersist()
        combOutput.flatMap(r => (r.array(0)))
          .union(covReduced)

  }
}

object CoverageReadFunctions {

  implicit def addCoverageReadFunctions(rdd: RDD[BAMRecord]) = {
    new CoverageReadFunctions(rdd)

  }
}

