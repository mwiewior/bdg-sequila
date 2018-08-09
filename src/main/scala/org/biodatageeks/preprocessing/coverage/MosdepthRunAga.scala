package org.biodatageeks.preprocessing.coverage

import org.apache.spark.sql.SparkSession

object MosdepthRunAga {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.driver.memory", "8g")
      .master("local[4]")
      .getOrCreate()
    import spark.implicits._
    import scala.collection.mutable.ArrayBuffer
    import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}


    // FULL SCHEMA WITH START, END

    spark.time {

      val schemaFull = new StructType()
        .add(StructField("contigName", StringType, true))
        .add(StructField("start", IntegerType, true))
        .add(StructField("end", IntegerType, true))
        .add(StructField("coverage", IntegerType, true))

      val mosBlocks = spark
        .read
        .format("csv")
        .option("header", false)
        .option("delimiter", "\t")
        .schema(schemaFull)
        //.load("file:///Users/aga/workplace/data/mos.per-base.bed.slice") // FIXME, copy file has cut-out other chromosomes
        .load("file:///Users/marek/data/mos.per-base.bed2.bed")

      val mosRdd = mosBlocks.rdd

      // val bases = mosRdd
      //  .flatMap { r => {
      //   val start = r.getInt(1)
      //   val end = r.getInt(2)
      //   val array = ArrayBuffer[(String, Int, Int, Int)]()
      //   for (ind <- start to end-1 ) {
      //     array.append((r.getString(0), ind, ind, r.getInt(3)))
      //   }
      //   array
      //   }
      // }


      val bases = mosRdd
        .flatMap { r => {
          val chr = r.getString(0)
          val start = r.getInt(1)
          val end = r.getInt(2)
          val cov = r.getInt(3)

          val array = new Array[(String, Int, Int, Int)](end - start)

          var cnt = 0
          var position = start

          while (position < end) {
            array(cnt) = (chr, position, position, cov)
            cnt += 1
            position += 1
          }
          array
        }
        }
      println(bases.count() )
    }
  }
}
