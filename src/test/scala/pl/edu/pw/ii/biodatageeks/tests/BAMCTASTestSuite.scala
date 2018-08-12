package pl.edu.pw.ii.biodatageeks.tests

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
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
          |AS SELECT * FROM ${tableNameBAM} WHERE sampleId='NA12878' and contigName='20' AND start >1000 LIMIT 10
        """.stripMargin)
           .show()
          //.explain(true)

    ss
    .sql(s"DESC FORMATTED  bam_ctas")
    .show(1000,false)



  }

  after{

  }
}
