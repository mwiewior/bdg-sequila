package pl.edu.pw.ii.biodatageeks.tests

import org.apache.spark.sql.SequilaSession
import org.biodatageeks.utils.SequilaRegister

class SequilaDatasourceStrategyTestSuite  extends  BAMBaseTestSuite {

  test("Simple select over a BAM table") {
    val  ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss
      .sql(s"SELECT * FROM ${tableNameBAM}")
      .explain(true)
  }

}
