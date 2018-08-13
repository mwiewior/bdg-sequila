package pl.edu.pw.ii.biodatageeks.tests

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import htsjdk.samtools.SAMRecord
import org.apache.spark.sql.SequilaSession
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.biodatageeks.utils.SequilaRegister
import org.scalatest.{BeforeAndAfter, FunSuite}

class BAMCTASTestSuite  extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext{

  val bamPath = getClass.getResource("/NA12878.slice.bam").getPath
  val bamCTAS =  getClass.getResource("/ctas").getPath
  val tableNameBAM = "reads"


  before {
    spark.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
    spark.sql(
      s"""
         |CREATE TABLE ${tableNameBAM}
         |USING org.biodatageeks.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamPath}")
         |
      """.stripMargin)
  }

  test("BAM - CTAS" ){
    val  ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss
      .sql(
        s"""
          |CREATE TABLE bam_ctas USING org.biodatageeks.datasources.BAM.BAMDataSource
          |OPTIONS(path "${bamCTAS}/*.bam")
          |AS SELECT * FROM ${tableNameBAM} WHERE sampleId='NA12878'
        """.stripMargin)
           .show()
          //.explain(true)

    ss
    .sql(s"DESC FORMATTED  bam_ctas")
    .show(1000,false)

    val dfSrc = ss.sql(s"SELECT contigName,start,end FROM ${tableNameBAM} WHERE contigName='chr1' AND start>390 ORDER BY contigName, start")
    println(dfSrc.count())
    val dfDst = ss.sql(s"SELECT contigName,start,end FROM bam_ctas WHERE contigName='chr1' AND start>390 ORDER BY contigName, start")
    println(dfDst.count())
    assertDataFrameEquals(dfSrc,dfDst)



  }

  after{

  }
}
