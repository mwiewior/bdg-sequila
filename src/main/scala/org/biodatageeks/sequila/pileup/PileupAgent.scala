package org.biodatageeks.sequila.pileup

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute


case class PileupRecord (contig: String, pos: Int, ref: String, cov: Int)


class PileupAgent(spark:SparkSession) {
  val logger: Logger = Logger.getLogger(this.getClass.getCanonicalName)

  def calculatePileup(tableName: String, output:Seq[Attribute]): RDD[PileupRecord] = {
    logger.info(s"Calculating pileup on table: $tableName")
    // dummy implementation
    val rdd = spark.sparkContext.parallelize(Seq(1,2,3,4,5,6,7,8,9,10))
    val out = rdd.map(r=>PileupRecord("1", r, "A", (r + 10).toShort ))
    out
  }

}
