package org.hammerlab.sparkbench

import org.apache.spark.{SparkContext, SparkConf}

object ReferenceStatistics extends App {

  override def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("ReferenceStatistics")
    val sc = new SparkContext(conf)
    
    val referenceFile = args(1)
    val substringLength = args(2).toInt
    val outputPath = args(3)
    
    val lines = sc.textFile(referenceFile).zipWithIndex()

    val substringsAndPositions =
      lines
        .flatMap(
          lineAndIndex =>
            lineAndIndex._1
              .sliding(substringLength)
              .zipWithIndex
              .map(s => (s._1, (s._2, lineAndIndex._2)))
        )

    val substringCounts = 
      substringsAndPositions
      .map(kv => (kv._1, 1L))
      .reduceByKey(_ + _)

    substringCounts
      .map(kv => s"${kv._1},${kv._2}")
      .saveAsTextFile(outputPath)
    
  }
}
