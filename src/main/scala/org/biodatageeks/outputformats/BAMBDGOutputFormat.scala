package org.biodatageeks.outputformats

import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.seqdoop.hadoop_bam.{
  KeyIgnoringBAMOutputFormat,
  KeyIgnoringBAMRecordWriter,
  SAMRecordWritable
}
import org.apache.hadoop.fs.Path

class BAMBDGOutputFormat[K] extends KeyIgnoringBAMOutputFormat[K] with Serializable {
  setWriteHeader(true)

  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[K, SAMRecordWritable] = {
    val conf = context.getConfiguration()

    // source BAM file to get the header from and the output BAM for writing
    val inPath = new Path(conf.get("spark.biodatageeks.bam.bam_header_path"))
    val outPath = new Path(conf.get("spark.biodatageeks.bam.output_path"))

    readSAMHeaderFrom(inPath, conf)

    // now that we have the header set, we need to make a record reader
   new KeyIgnoringBAMRecordWriter[K](outPath,header, true, context)

  }
}