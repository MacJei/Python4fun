import pandas as pds
import datetime as dt
import glob
import os
import s3fs
from pyspark import SparkContext, sql
def Spark_read_write_csv_to_hdfs(inputType, fileList, outDirectory):
    sc = SparkContext(appName="DATA-local-to-HDFS")
    #Set out put replication factor to 1
    sc._jsc.hadoopConfiguration().set("dfs.replication", "1")
    sqlContext = sql.SQLContext(sc)
    for filename in fileList:
        print 'Reading ' + 'file://' + filename
        rddFrame1 = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true') \
        .load('file://' + filename)
        #rddFrame1.coalesce(1).write.format('com.databricks.spark.csv').save(outDirectory+filename[len(filename)-33:])
        rddFrame1.write.format('com.databricks.spark.csv').save(
            outDirectory + filename[len(filename) - 33:])
        print 'Writing '+outDirectory+filename[len(filename)-33:]+ ' done!'
    sc.stop()
if __name__ == "__main__":

    intDirectory = '/home/ec2-user/sample-data-eri-ca/'
    outDirectory = "hdfs://hdfs1:8020/user/ec2-user/sample-data-ca/"
    #outDirectory = os.path.join(os.path.dirname(__file__), 'sample-data-ca/')
    #s3bucket = "sample-data-e"  # your s3bucket name
    #s3Files = s3fs.S3FileSystem(anon=False).ls(s3bucket)
    localFiles = glob.glob(intDirectory + '*.csv')
    Spark_read_write_csv_to_hdfs("local", localFiles, outDirectory)
    #print Spark_read_write_csv_to_hdfs("s3", localFiles, outDirectory)


#spark-submit --packages com.databricks:spark-csv_2.10:1.5.0 Spark-1.6-read-write-CSV-to-hdfs-yarn-CDH5.py