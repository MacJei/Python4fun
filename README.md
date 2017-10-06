# spark-pandas-s3
Spark & Pandas batch processing demo, data will be loaded from local & AWS s3

Quickstart
----------

(1) run locally, by doing this you are expected to first command out the hdfs part

    python spark-pandas.py
    

(2) deploy to the spark cluster, by doing this you are expected to first command out the pandas part

    $SPARK_HOME/bin/spark-submit --master spark://ec2-34-208-33-205.us-west-2.compute.amazonaws.com:7077 --deploy-mode cluster --executor-memory 1g spark-pandas-s3-hdfs.py
