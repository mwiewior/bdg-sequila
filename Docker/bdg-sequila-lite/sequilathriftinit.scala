import org.apache.spark.sql.SequilaSession
import org.biodatageeks.utils.{SequilaRegister, UDFRegister}


val ss = SequilaSession(spark)
UDFRegister.register(ss)

/*inject bdg-granges strategy*/
SequilaRegister.register(ss)

ss.sparkContext.hadoopConfiguration.set("hive.metastore.warehouse.dir","/data/input/bams")

ss.sql(
  """
    |CREATE TABLE IF NOT EXISTS reads
    |USING org.biodatageeks.datasources.BAM.BAMDataSource
    |OPTIONS(path "/data/input/bams/*.bam")
  """.stripMargin)

System.exit(0)

