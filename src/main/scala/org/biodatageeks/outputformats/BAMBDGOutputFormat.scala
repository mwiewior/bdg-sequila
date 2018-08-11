package org.biodatageeks.outputformats

import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.seqdoop.hadoop_bam.{KeyIgnoringBAMOutputFormat, KeyIgnoringBAMRecordWriter, SAMRecordWritable}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.biodatageeks.utils.BDGInternalParams

class BAMBDGOutputFormat extends KeyIgnoringBAMOutputFormat[NullWritable] with Serializable {
  setWriteHeader(true)

  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[NullWritable, SAMRecordWritable] = {
    val conf = context.getConfiguration()

    // source BAM file to get the header from and the output BAM for writing
    val inPath = new Path(conf.get(BDGInternalParams.BAMCTASHeaderPath))
    val outPath = new Path(conf.get(BDGInternalParams.BAMCTASOutputPath))

    readSAMHeaderFrom(inPath, conf)

    // now that we have the header set, we need to make a record reader
   new KeyIgnoringBAMRecordWriter[NullWritable](outPath,header, true, context)

  }
}