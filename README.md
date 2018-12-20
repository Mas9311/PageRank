# Apache Spark Page Rank #

A simple page ranker (with and without taxation) that can run in a local client or in a cluster.

---------------

### Requirements: ###

In order to run, you will first need [Apache Spark 2.3.2](https://spark.apache.org/docs/2.3.2/) which can be downloaded from [here](https://www.apache.org/dyn/closer.lua/spark/spark-2.3.2/spark-2.3.2-bin-hadoop2.7.tgz) for 215 MB. <br>
1. You will need to update your environment variables for $SPARK_HOME and I also advise you set up your spark-defaults.sh and spark-env.conf files in /conf directory of spark.
1. [Download](https://github.com/Mas9311/page-rank/archive/master.zip) this repository and unzip the directory.
1. Pick one of the two from the Usage Section below.
    - if you have a cluster set up, use the first
    - else, run it locally

---------------

### Usage: ###

###### cluster: ######
<pre>
$SPARK_HOME/bin/spark-submit --class com.pagerank.scala.PageRank --master spark://HOST:PORT --deploy-mode cluster ./out/artifacts/PR_jar/PR.jar hdfs:HOST:HDFS_PORT/path/to/links.txt hdfs:HOST:HDFS_PORT/path/to/titles.txt
</pre>

###### local: ######
<pre>
$SPARK_HOME/bin/spark-submit --class com.pagerank.scala.PageRank --master local --deploy-mode client ./out/artifacts/PR_jar/PR.jar file:///local/path/to/links.txt file:///local/path/to/titles.txt
</pre>
