package org.biodatageeks.sequila.tests.pileup

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.tests.base.BAMBaseTestSuite
import org.biodatageeks.sequila.utils.{Columns, SequilaRegister}

class MDTagTestSuite extends BAMBaseTestSuite with SharedSparkContext{

  test("MD Tags"){
    val  ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    val query =
      s"""
         |SELECT count(*),tag_MD
         |FROM  $tableNameBAM group by tag_MD order by count(*) desc
         |
       """.stripMargin
    ss
      .sql(query)
      .show(5,false)

    val query2 =
      s"""
         |SELECT count(tag_MD)
         |FROM  $tableNameBAM where tag_MD not rlike '^[0-9]*$$' and tag_md is not null
         |
       """.stripMargin
    ss
      .sql(query2)
      .show(5,false)
  }

  test("Check"){

  }

}
