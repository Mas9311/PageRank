
# Apache Spark Page Rank #

A simple page ranker (with and without taxation) that can run in a local client or in a cluster.

---------------

### Requirements: ###

In order to run, you will first need [Apache Spark 2.3.2](https://spark.apache.org/docs/2.3.2/) which can be downloaded from [here](https://www.apache.org/dyn/closer.lua/spark/spark-2.3.2/spark-2.3.2-bin-hadoop2.7.tgz) for 215 MB.
<br />
Next, open the command line and type the following command to download this repo:
<pre> wget https://github.com/Mas9311/page-rank.git </pre>

---------------

### Usage: ###

###### local: ######
<pre>
$SPARK_HOME/bin/spark-submit \
    --class com.pagerank.scala.PageRank \
    --master local \
    --deploy-mode client \
    /local/path/to/out/artifact/PR_jar/PR.jar \
    file:///local/path/to/links.txt \
    file:///local/path/to/titles.txt
</pre>

###### cluster: ######
<pre>
$SPARK_HOME/bin/spark-submit \
    --class com.pagerank.scala.PageRank \
    --master <spark://HOST:PORT> \
    --dir 
    --deploy-mode <cluster> \
    /local/path/to/out/artifact/PR_jar/PR.jar \
    hdfs:HOST:HDFS_PORT/path/to/links.txt \
    hdfs:HOST:HDFS_PORT/path/to/titles.txt
</pre>
