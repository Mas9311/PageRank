package com.pagerank.scala

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
//
//    val numericalOrderIdeal = idealPageRanks.takeOrdered(idealPageRanks.count.toInt)(Ordering[String].reverse.on(x => x._1))
//    val numericalOrderTaxed = taxedPageRanks.takeOrdered(taxedPageRanks.count.toInt)(Ordering[String].reverse.on(x => x._1))
//
//    println("    Ideal")
//    numericalOrderIdeal.foreach(tup => println(s"${tup._1} \t${tup._2}"))
//    println()
//    println("    Taxed")
//    numericalOrderTaxed.foreach(tup => println(s"${tup._1} \t${tup._2}"))

    val indexedTitles = inputTitles.zipWithIndex.map{ x =>
      (x._2 + 1, x._1) }

    val z = indexedTitles.map { case (index, title) => (index.toInt, title)}
    val i = idealPageRanks.map { case (index, rank) => (index.toInt, rank)}
    val t = idealPageRanks.map { case (index, rank) => (index.toInt, rank)}

    val joinedIdeal = z.fullOuterJoin(i).map {
      case (title, rank) => (title, rank)
    }
    joinedIdeal.foreach(println)

    val joinedTaxed = z.fullOuterJoin(i).map {
      case (title, rank) => (title, rank)
    }
    joinedTaxed.foreach(println)

    var topN = 10
    if (idealPageRanks.count.toInt < topN)
      topN = idealPageRanks.count.toInt

    val orderedIdeal = idealPageRanks.takeOrdered(topN)(Ordering[Double].reverse.on(x => x._2))
    val orderedTaxed = taxedPageRanks.takeOrdered(topN)(Ordering[Double].reverse.on(x => x._2))

    println(s"***************************************************************")
    println("\tIdeal Page Ranks (without taxation):")
    orderedIdeal.foreach(tup => println(s"${tup._1} has rank\t${tup._2}"))

    println("\n\tPage Ranks with taxation:")
    orderedTaxed.foreach(tup => println(s"${tup._1} has rank\t${tup._2}"))
    println(s"***************************************************************")
//
//    val sc = ss.sparkContext
//    val fs = FileSystem.get(sc.hadoopConfiguration)
//    val outputPath = "hdfs://phoenix:30381/output"
//
//    if(fs.exists(new Path(outputPath)))
//      fs.delete(new Path(outputPath),true)
//
//    orderedIdeal.coalesce(1).saveAsTextFile(outputPath + "/t")
//    orderedTaxed.coalesce(1).saveAsTextFile(outputPath + "/i")
    
    ss.stop()
    
  }

}
