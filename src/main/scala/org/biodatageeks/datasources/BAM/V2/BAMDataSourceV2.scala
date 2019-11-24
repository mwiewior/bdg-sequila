package org.biodatageeks.datasources.BAM

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

class BAMDataSourceV2  extends DataSourceV2  with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = ???
}
