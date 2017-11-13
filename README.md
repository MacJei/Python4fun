# spark-pandas-s3
Spark & Pandas batch processing demo, data will be loaded from local, remote s3 and HDFS.

Quickstart
----------

(1) Run locally

    python spark-pandas-hdfs-s3.py
    

(2) Subimit job to your spark stand-alone cluster, if you already have one:)

    $SPARK_HOME/bin/spark-submit --master spark://node1:7077 --deploy-mode cluster --executor-memory 1g spark-pandas-hdfs-s3.py


(2) Submit job to yarn on top of your hdfs cluster, if you already have one:)

    $spark-submit --master yarn --deploy-mode cluster --executor-memory 1g --packages com.databricks:spark-csv_2.10:1.5.0 spark-CDH5-1.6-hdfs-yarn.py


The logs you may want to expect in your local testing:

![img](https://s3-us-west-2.amazonaws.com/github-photo-links/Screen+Shot+2017-10-06+at+2.35.06+PM.png)
![img](https://s3-us-west-2.amazonaws.com/github-photo-links/Screen+Shot+2017-10-06+at+2.37.56+PM.png)
