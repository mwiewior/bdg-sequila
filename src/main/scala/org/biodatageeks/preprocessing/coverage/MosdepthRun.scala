package org.biodatageeks.preprocessing.coverage

import org.apache.spark.sql.SparkSession

object MosdepthRun {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.driver.memory", "8g")
      .master("local[4]")
      .getOrCreate()

      val path = "file:///data/samples/coverage/mos.per-base.bed2.bed"
      spark.time {
        val mosRes = spark
          .sparkContext
          .textFile(path)
          .map(_.split('\t'))
          .cache()

        val maxContigPos = mosRes
          .map(_ (2).toInt)
          .reduce((a, b) => math.max(a, b))

        val perBaseCov = mosRes
          .mapPartitions(p => {

            val res = new Array[(String, Int, Int)](maxContigPos)
            var i = 0
            while (p.hasNext) {
              val row = p.next()
              var k = row(1).toInt
              val maxBlock = row(2).toInt
              while(k < maxBlock){
                res(i) = (row(0),k,row(3).toInt)
                k += 1
                i += 1
              }

            }

            res
              .take(i)
              .toIterator
          }
          )
      println(perBaseCov.count())
      //perBaseCov.take(10).foreach(println(_))
      }
  }

}
