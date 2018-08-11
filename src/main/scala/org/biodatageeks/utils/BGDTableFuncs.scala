package org.biodatageeks.utils

import org.apache.spark.sql.SparkSession

object BDGTableFuncs{

  def getTableMetadata(spark:SparkSession, tableName:String) = {
    val catalog = spark.sessionState.catalog
    val tId = spark.sessionState.sqlParser.parseTableIdentifier(tableName)
    catalog.getTableMetadata(tId)
  }
}