import glob
import hdfs
from pyspark import SparkContext
from pyspark.sql import SQLContext

def Spark_join_csv_in_hdfs(inputType, fileList, hdfs_dimension_File, hdfs_fact_Files, intDirectory, outDirectory):
    sc = SparkContext(appName="DATA-JOIN-HDFS")
    #Set out put replication factor to 1
    #sc._jsc.hadoopConfiguration().set("dfs.replication", "1")
    #let program comunicate hdfs blocks from remote
    sc._jsc.hadoopConfiguration().set("dfs.client.use.datanode.hostname", "true")
    sqlContext = SQLContext(sc)
    rdd_dimension = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true') \
        .load(hdfs_dimension_File)
    for filename in hdfs_fact_Files:
        print 'Reading ' + intDirectory + filename
        df_fact = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true') \
        .load(intDirectory + filename).select('EUTRANCELLFDD','DATETIME','PMACTIVEUEDLSUM')
        #condition = [df.name == df3.name, df.age == df3.age]
        outputDF = df_fact.join(rdd_dimension, df_fact.EUTRANCELLFDD == rdd_dimension.EUTRANCELLFDD, 'left').select(df_fact.EUTRANCELLFDD,'DATETIME', rdd_dimension.REGION, rdd_dimension.MARKET, 'PMACTIVEUEDLSUM')
        outputDF.show()
        outputDF.write.format('com.databricks.spark.csv').save('/Users/Joy4fun/Desktop/joined_'+filename)
        #output to hdfs
        #outputDF.write.format('com.databricks.spark.csv').save(outDirectory + filename)
        print 'Writing is done!'
    sc.stop()
if __name__ == "__main__":

    intDirectory = "hdfs://hdfs2:8020/user/ec2-user/sample_data_eri/"
    outDirectory = "hdfs://hdfs2:8020/user/ec2-user/joined_data/"

    hdfs_fact_Files = hdfs.Client('http://hdfs2:50070').list('/user/ec2-user/sample_data_eri/')
    hdfs_dimension_File = "hdfs://hdfs2:8020/user/ec2-user/ERI_CELL_REGION_MARKET.csv"
    Spark_join_csv_in_hdfs("local", hdfs_fact_Files, hdfs_dimension_File, hdfs_fact_Files, intDirectory, outDirectory)
    #print Spark_read_write_csv_to_hdfs("s3", localFiles, outDirectory)


#spark-submit --packages com.databricks:spark-csv_2.10:1.5.0 Spark-1.6-read-write-CSV-to-hdfs-yarn-CDH5.py

#pyspark --packages com.databricks:spark-csv_2.11:1.5.0

#If you don't want to give --packages option, please:
    # sudo cp downloads/spark-csv_2.11-1.5.0.jar /Library/Python/2.7/site-packages/pyspark/jars/.