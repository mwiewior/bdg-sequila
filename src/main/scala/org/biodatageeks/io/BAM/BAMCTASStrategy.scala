package org.biodatageeks.io.BAM

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery, SubqueryAlias}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.biodatageeks.datasources.BDGInputDataType
import org.biodatageeks.utils.{BDGInternalParams, BDGTableFuncs}

class BAMCTASStrategy(spark: SparkSession) extends Strategy with Serializable  {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case CreateDataSourceTableAsSelectCommand(table,mode, query) => {
      val srcTable = query.collect { case r: SubqueryAlias => r.alias }.head
      val srcTableMeta = BDGTableFuncs
        .getTableMetadata(spark,srcTable)
      srcTableMeta.provider match{
        case Some(f) => {
          if (f == BDGInputDataType.BAMInputDataType) {
              val tableDir = BDGTableFuncs.getTableDirectory(spark,srcTable)
              println(tableDir)
              spark
              .sqlContext
              .setConf(BDGInternalParams.BAMCTASDir, tableDir)
          }
          else Nil

        }
        case _ => Nil
      }
      Nil
    }

    case _ => Nil

  }

}
