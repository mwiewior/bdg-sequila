package org.biodatageeks.sequila.tests.pileup

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.tests.base.BAMBaseTestSuite
import org.biodatageeks.sequila.utils.SequilaRegister

class PileupTestSuite extends BAMBaseTestSuite with SharedSparkContext {

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
    assert(result.head.getString(0) == "1")

  }
}
