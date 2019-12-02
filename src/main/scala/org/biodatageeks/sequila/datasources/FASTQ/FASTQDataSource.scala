package org.biodatageeks.sequila.datasources.FASTQ

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.biodatageeks.sequila.datasources.BAM.BDGAlignmentRelation
import org.seqdoop.hadoop_bam.BAMBDGInputFormat

class FASTQDataSource extends DataSourceRegister with RelationProvider {
  override def shortName(): String = "fastq"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    new SequencedFragmentRelation(parameters("path"))(sqlContext)
  }

}
