package org.biodatageeks.utils

import org.apache.spark.sql.SparkSession
import org.biodatageeks.io.BAM.BAMCTASStrategy
import org.biodatageeks.preprocessing.coverage.CoverageStrategy
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim

object SequilaRegister {

  def register(spark : SparkSession) = spark.experimental.extraStrategies =
    Seq(
      new IntervalTreeJoinStrategyOptim(spark),
      new CoverageStrategy(spark),
      new BAMCTASStrategy(spark)
    )
}
