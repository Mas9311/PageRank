package com.pagerank.scala

import org.apache.spark.sql.SparkSession

object PageRank {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("Spark Ideal PageRank")
      .getOrCreate()

    val lines = spark.read.textFile(args(0)).rdd

    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to 25) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _)
//      with taxation:
//      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val finalRank = ranks.collect()

    finalRank.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))

    spark.stop()

  }

}