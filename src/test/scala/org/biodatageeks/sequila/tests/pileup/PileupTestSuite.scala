package org.biodatageeks.sequila.tests.pileup

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.tests.base.BAMBaseTestSuite
import org.biodatageeks.sequila.utils.SequilaRegister
import org.scalatest.{BeforeAndAfter, FunSuite}

class PileupTestSuite  extends FunSuite
  with DataFrameSuiteBase
  with BeforeAndAfter
  with SharedSparkContext {

  val bamMultiPath: String =
    getClass.getResource("/multichrom/bam/NA12878.multichrom.bam").getPath

  val tableNameMultiBAM = "readsMulti"

  val bamPath: String = getClass.getResource("/NA12878.slice.md.bam").getPath
  val tableNameBAM = "reads"


  before {

    spark.sql(s"DROP TABLE IF EXISTS $tableNameBAM")
    spark.sql(s"""
                 |CREATE TABLE $tableNameBAM
                 |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
                 |OPTIONS(path "$bamPath")
                 |
      """.stripMargin)

    spark.sql(s"DROP TABLE IF EXISTS $tableNameMultiBAM")
    spark.sql(s"""
                 |CREATE TABLE $tableNameMultiBAM
                 |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
                 |OPTIONS(path "$bamMultiPath")
                 |
      """.stripMargin)
  }

  test("Pileup mock"){
    val  ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sparkContext.setLogLevel("INFO")
    val query =
      s"""
         |SELECT *
         |FROM  pileup('$tableNameBAM')
       """.stripMargin
    val result = ss.sql(query)
    result.show(5,false)
    assert(result.count() == 2899)

  }


  test("Pileup multi"){
    val  ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sparkContext.setLogLevel("INFO")
    val query =
      s"""
         |SELECT distinct contig
         |FROM  pileup('$tableNameMultiBAM')
       """.stripMargin
    val result = ss.sql(query)
    assert(result.count() == 2)


  }
}
