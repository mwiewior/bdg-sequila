package org.biodatageeks.preprocessing.coverage

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeProjection, GenerateUnsafeRowJoiner}
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Coverage, Row, SparkSession, Strategy}
import org.apache.spark.unsafe.types.UTF8String


class CoverageStrategy(spark: SparkSession) extends Strategy with Serializable  {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case Coverage(tableName,output) => CoveragePlan(plan,spark,tableName,output) :: Nil
    case _ => Nil
  }

}

case class CoveragePlan(plan: LogicalPlan, spark: SparkSession, table:String, output: Seq[Attribute]) extends SparkPlan with Serializable {
  def doExecute(): org.apache.spark.rdd.RDD[InternalRow] = {
    val df = spark.sql(s"select sampleId,contigName,start,1 FROM ${table}")
      .limit(10)
    val schema = plan.schema
    df.rdd
        .map(r=>Row.fromSeq(Seq(
          UTF8String.fromString(r.getString(0)),
          UTF8String.fromString(r.getString(1)),
            r.getInt(2),
            r.getInt(3)
        ) ) )
      .map(r=>  UnsafeProjection.create(schema).apply(InternalRow.fromSeq(r.toSeq)))
  }

  def children: Seq[SparkPlan] = Nil
}