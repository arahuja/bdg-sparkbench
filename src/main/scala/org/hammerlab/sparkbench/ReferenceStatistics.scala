package org.hammerlab.sparkbench

import org.apache.spark.{SparkConf, SparkContext}
import org.kohsuke.args4j.{CmdLineParser, Option => Args4jOption}

object ReferenceStatistics extends App {
  
  @Args4jOption(name="--reference", required = true, usage="HDFS path to FASTA file")
  var referencepath: String = ""

  @Args4jOption(name="--outputPath", required = true, usage="Output path to save substring counts")
  var outputPath: String = ""

  @Args4jOption(name="--substringLength", usage="Length of substrings to create")
  var substringLength: Int = 30

  @Args4jOption(name="--sort", usage="Sort the substrings by count before outputing")
  var sort: Boolean = false

  override def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("ReferenceStatistics")
    val sc = new SparkContext(conf)

    val parser = new CmdLineParser(this)
    
    val lines = sc
      .textFile(referencepath)
      // Skip contig description lines
      .filter(line => !line.startsWith(">"))
      .zipWithIndex()

    val substringsAndPositions =
      lines
        .flatMap(
          lineAndIndex =>
            lineAndIndex._1
              .sliding(substringLength)
              .zipWithIndex
              .map(s => (s._1, (s._2, lineAndIndex._2)))
        )

    var substringCounts = 
      substringsAndPositions
      .map(kv => (kv._1, 1L))
      .reduceByKey(_ + _)

    substringCounts = 
      if (sort) 
        substringCounts.sortBy(_._2)
      else
        substringCounts

    substringCounts
      .map(kv => s"${kv._1},${kv._2}")
      .saveAsTextFile(outputPath)
    
  }
}
