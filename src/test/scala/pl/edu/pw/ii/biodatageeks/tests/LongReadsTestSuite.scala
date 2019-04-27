package pl.edu.pw.ii.biodatageeks.tests

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.bdgenomics.utils.instrumentation.Metrics
import org.biodatageeks.utils.{BDGInternalParams, SequilaRegister}
import org.scalatest.{BeforeAndAfter, FunSuite}

class LongReadsTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext {

  val bamPath = getClass.getResource("/nanopore_guppy_slice.bam").getPath
  //val bamPath = "/Users/marek/data/guppy.chr21.bam"
  val splitSize = 30000
  val tableNameBAM = "reads"

  before {

    System.setSecurityManager(null)
    spark.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
    spark.sql(
      s"""
         |CREATE TABLE ${tableNameBAM}
         |USING org.biodatageeks.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamPath}")
         |
      """.stripMargin)

  }
//  test("BAM - Nanopore with guppy basecaller") {
//
//    spark.sqlContext.setConf(BDGInternalParams.InputSplitSize, splitSize)
//
//    val session: SparkSession = SequilaSession(spark)
//    SequilaRegister.register(session)
//    session
//      .sparkContext
//      .setLogLevel("INFO")
//    val bdg = session.sql(s"SELECT * FROM ${tableNameBAM}")
//    assert(bdg.count() === 150)
//  }

  test("BAM - coverage - Nanopore with guppy basecaller") {

    spark.sqlContext.setConf(BDGInternalParams.ShowAllPositions,"false")
    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)
    session
      .sparkContext
      .setLogLevel("WARN")
    val query =s"SELECT * FROM bdg_coverage('${tableNameBAM}','nanopore_guppy_slice','bases') order by contigName,start,end"
    session.sqlContext.setConf(BDGInternalParams.InputSplitSize, (splitSize*10).toString)
    val covOnePartitionDF = session.sql(query)//.take (10)//take(12180)//.drop(12150) // fails from 12153
    covOnePartitionDF.coalesce(1).write.mode("overwrite").option("delimiter", "\t").csv("/Users/aga/workplace/onePart.csv")

    val session2: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session2)
    session2.sqlContext.setConf(BDGInternalParams.InputSplitSize, splitSize.toString)
    val covMultiPartitionDF = session2.sql(query)//.take(10)//take(12180).drop(12150)
    covMultiPartitionDF.coalesce(1).write.mode("overwrite").option("delimiter", "\t").csv("/Users/aga/workplace/multiPart.csv")


    //println (s"${covOnePartitionDF.count()}, ${covMultiPartitionDF.count} ")
    //assert(covOnePartitionDF === covMultiPartitionDF)
    //assertDataFrameEquals(covOnePartitionDF.orderBy("contigName", "start"), covMultiPartitionDF.orderBy("contigName", "start"))
  }
}