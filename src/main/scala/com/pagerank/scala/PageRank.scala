package com.pagerank.scala

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.util.Try

object PageRank {

  def makeInt(s: String): Try[Int] = Try(s.trim.toInt)

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: PageRank </path/to/links.txt> </path/to/titles.txt>")
      System.exit(1)
    }

    // Hides all the job tracker info printed to console
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val ss = SparkSession
      .builder
      .appName("Scala Page Rank")
      .getOrCreate()

    val inputLinks = ss.read.textFile(args(0)).rdd
    val inputTitles = ss.read.textFile(args(1)).rdd

    val links = inputLinks.map{ s =>
      val parts = s.split(":\\s+")
      (parts(0), parts(1).split("\\s"))
    }.distinct().cache()
    
    var idealPageRanks = links.mapValues(v => 1.0)
    var taxedPageRanks = links.mapValues(v => 1.0)

    val beta = 0.85
    val power = 0.15

    // minimum of 25 iterations recommended (until the vector converges)
    for (i <- 1 to 25) {
      // First, calculate ideal page ranks (without taxation)
      val idealContribs = links.join(idealPageRanks).values.flatMap{ case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }
      idealPageRanks = idealContribs.reduceByKey(_ + _)
      
      // Next, calculate the taxed page ranks for this iteration
      val taxedContribs = links.join(taxedPageRanks).values.flatMap{ case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }
      taxedPageRanks = taxedContribs.reduceByKey(_ + _).mapValues(power + beta * _)
    }

    val indexedTitles = inputTitles.zipWithIndex.map{ x => ((x._2 + 1).toInt, x._1) }
    val i = idealPageRanks.map { x => (x._1.toInt, x._2) }
    val t = taxedPageRanks.map { x => (x._1.toInt, x._2) }

    var topN = 10
    if (idealPageRanks.count.toInt < topN)
      topN = idealPageRanks.count.toInt

    val sc = ss.sparkContext

    // Make index values based on the line number in the titles file
    val topIdeal = i.takeOrdered(topN)(Ordering[Double].reverse.on(x => x._2))
    val topTaxed = t.takeOrdered(topN)(Ordering[Double].reverse.on(x => x._2))


    val joinedIdeal = sc.parallelize(topIdeal).join(indexedTitles).map { x => (x._2._2, x._2._1) }
    val joinedTaxed = sc.parallelize(topTaxed).join(indexedTitles).map { x => (x._2._2, x._2._1) }

    val finalIdeal = joinedIdeal.sortBy(-_._2)
    val finalTaxed = joinedTaxed.sortBy(-_._2)

    println(s"***************************************************************")
    println("\tIdeal Page Ranks (without taxation):")
    finalIdeal.foreach(tup => println(s"${tup._1}:\t${tup._2}"))

    println("\n\tPage Ranks with taxation:")
    finalTaxed.foreach(tup => println(s"${tup._1}:\t${tup._2}"))
    println(s"***************************************************************")

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val outputPath = "hdfs://phoenix:30381/output"

    if(fs.exists(new Path(outputPath)))
      fs.delete(new Path(outputPath),true)

    finalIdeal.coalesce(1).saveAsTextFile(outputPath + "/t")
    finalTaxed.coalesce(1).saveAsTextFile(outputPath + "/i")

    ss.stop()
    
  }

}
