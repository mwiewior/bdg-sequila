package org.apache.spark.sql


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.analysis.{AliasViewChild, Analyzer, CleanupAliases, EliminateUnions, ResolveCreateNamedStruct, ResolveHints, ResolveInlineTables, ResolveTableValuedFunctions, ResolveTimeZone, SeQuiLaAnalyzer, SubstituteUnresolvedOrdinals, TimeWindowing, TypeCoercion, UpdateOuterReferences}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.internal.{SQLConf, SessionState}
import org.biodatageeks.preprocessing.coverage.CoverageStrategy

import scala.util.Random






case class MySparkSession(sparkSession: SparkSession) extends SparkSession(sparkSession.sparkContext) {
  val myAnalyzer = new SeQuiLaAnalyzer(sparkSession.sessionState.catalog,sparkSession.sessionState.conf)
  def executePlan(plan:LogicalPlan) =  new QueryExecution(sparkSession,myAnalyzer.execute(plan))
  override lazy val sessionState = MySessionState(sparkSession,myAnalyzer,executePlan)
}

case class MySessionState(sparkSession: SparkSession, customAnalyzer: Analyzer,executePlan: LogicalPlan => QueryExecution)
  extends SessionState(sparkSession.sharedState,
    sparkSession.sessionState.conf,
    sparkSession.sessionState.experimentalMethods,
    sparkSession.sessionState.functionRegistry,
    sparkSession.sessionState.udfRegistration,
    sparkSession.sessionState.catalog,
    sparkSession.sessionState.sqlParser,
    customAnalyzer,
    sparkSession.sessionState.optimizer,
    sparkSession.sessionState.planner,
    sparkSession.sessionState.streamingQueryManager,
    sparkSession.sessionState.listenerManager,
    sparkSession.sessionState.resourceLoader,
    executePlan,
    (sparkSession:SparkSession,sessionState: SessionState) => sessionState.clone(sparkSession)){
}

object MyRule extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    println("yoooo działa PRINTLN!!!")
    // TODO: make some changes to LogicalPlan :)
    plan
  }
}

object UTVFRegister {

  def main(args: Array[String]): Unit = {

    System.setSecurityManager(null)
    type ExtensionsBuilder = SparkSessionExtensions => Unit
   // val f: ExtensionsBuilder = { e => e.injectResolutionRule(ResolveTableValuedFunctionsSeq) }
    val spark = SparkSession.builder()
      .master("local[1]")
     // .withExtensions(f)
      .getOrCreate()
//
//    spark
//      .sql("select * from range(1,2)")
//        .explain(true)

    //val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkathon")
    //val context: SparkContext = new SparkContext(conf)
    val session: SparkSession = MySparkSession(spark)
    session.sparkContext.setLogLevel("INFO")
    session.experimental.extraStrategies = new CoverageStrategy(session) :: Nil
    session.sql("select * from coverage('test')").explain(true)
    session.sql("select * from coverage('test')").explain(true)
    session.sql("select * from coverage('test')").explain(true)
  }

}
