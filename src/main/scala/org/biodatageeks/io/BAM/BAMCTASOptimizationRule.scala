package org.biodatageeks.io.BAM

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, RunnableCommand}
import org.apache.spark.sql.execution.datasources.CreateTable
import org.biodatageeks.datasources.BDGInputDataType

case class CreateBAMDataSourceTableAsSelectCommand(
                                                 table: CatalogTable,
                                                 mode: SaveMode,
                                                 query: LogicalPlan)
  extends RunnableCommand {

  override protected def innerChildren: Seq[LogicalPlan] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    println("Custom BAM")
    Seq.empty[Row]
  }

}



class BAMCTASOptimizationRule(spark: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case CreateTable(table, mode, Some(query))   => {
      println("Optimizing")
      CreateBAMDataSourceTableAsSelectCommand(table, mode, query)
      //CreateTable(table, mode, None)
    }
  }

}

