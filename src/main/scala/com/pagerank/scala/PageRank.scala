package com.pagerank.scala

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object PageRank {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: PageRank </path/to/links.txt> </path/to/titles.txt>")
      System.exit(1)
    }

    val ss = SparkSession
      .builder
      .appName("Scala Page Rank")
      .getOrCreate()

    val inputLinks = ss.read.textFile(args(0)).rdd
    val inputTitles = ss.read.textFile(args(0)).rdd

    val links = inputLinks.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    
    var idealPageRanks = links.mapValues(v => 1.0)
    var taxedPageRanks = links.mapValues(v => 1.0)
    
    val beta = 0.85
    val power = 1 - beta
    println(s"beta:  ${beta}")
    println(s"power: ${power}")

    // minimum of 25 iterations recommended (until the vector converges)
    for (i <- 1 to 25) {
      // First, calculate ideal page ranks (without taxation)
      val contribs = links.join(idealPageRanks).values.flatMap{ case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }
      idealPageRanks = contribs.reduceByKey(_ + _).mapValues(_)
      
      // Next, calculate the taxed page ranks for this iteration
      val contribs = links.join(taxedPageRanks).values.flatMap{ case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }
      taxedPageRanks = contribs.reduceByKey(_ + _).mapValues(_ * beta + power)
    }

    val idealFinalRanks = idealPageRanks.collect()
    val taxedFinalRanks = taxedPageRanks.collect() // or .coalesce(1) ?
    
    println("Page Ranks (without taxation):")
    idealFinalRanks.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))
    
    println("Ideal Page Ranks with taxation:")
    taxedFinalRanks.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))
    
    // if output directory exists in hdfs:
    //   delete it
    // write final ideal ranks to textFile.
    // write final taxed ranks to textFile.
    
    ss.stop()
    
  }

}
