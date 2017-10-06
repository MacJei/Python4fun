# spark-pandas-s3
Spark & Pandas batch processing demo, data will be loaded from local, remote s3 and HDFS.

Quickstart
----------

(1) run locally

    python spark-pandas-hdfs-s3.py
    

(2) deploy to the spark cluster, by doing this you are expected to first command out the pandas part

    $SPARK_HOME/bin/spark-submit --master spark://ec2-34-208-33-205.us-west-2.compute.amazonaws.com:7077 --deploy-mode cluster --executor-memory 1g spark-pandas-hdfs-s3.py
