package com.pagerank.scala

import org.apache.spark.SparkContext

object PageRank {

  def main(args: Array[String]) {
    val hdfsPath = "hdfs://phoenix:30381"

    //Create a SparkContext to initialize Spark
//    val conf = new SparkConf()
//    conf.setMaster("local")
//    conf.setAppName("Word Count")
//    val sc = new SparkContext(conf)
    val sc = new SparkContext()

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val inputFile = sc.textFile(hdfsPath + "/input/h")

    //word count
    val counts = inputFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreach(println)
    counts.saveAsTextFile(hdfsPath + "/h")
  }

}