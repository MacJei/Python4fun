import hdfs
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row, functions as sqlf
from pyspark.sql.window import Window
# some best practice:

# >>> rdd = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('hdfs://hdfs1:8020/user/ec2-user/ERI_CELL_REGION_MARKET.csv')

# >>> rdd.show(5)
# +-------------+-------+-----------+
# |EUTRANCELLFDD| REGION|     MARKET|
# +-------------+-------+-----------+
# |SXL03047_7A_1|Central|San Antonio|
# |SXL03047_7C_1|Central|San Antonio|
# |SXL03049_7A_1|Central|San Antonio|
# |SXL03049_7C_1|Central|San Antonio|
# |SXL03053_7A_1|Central|San Antonio|
# +-------------+-------+-----------+
# only showing top 5 rows

# >>> import pyspark.sql.functions as sqlf

# >>> rdd.select("REGION","MARKET").groupBy("REGION").agg(sqlf.countDistinct("MARKET")).show()
# +---------+-------------+
# |   REGION|count(MARKET)|
# +---------+-------------+
# |  Unknown|            1|
# |Southeast|           38|
# |     West|            8|
# |  Central|           15|
# |Northeast|           15|
# |         |            1|
# +---------+-------------+
#
# >>> rdd.select("REGION","MARKET").groupBy("REGION").agg(sqlf.countDistinct("MARKET").alias('MARKET_COUNT')).orderBy(sqlf.desc('MARKET_COUNT')).show(3)
# +---------+------------+
# |   REGION|MARKET_COUNT|
# +---------+------------+
# |Southeast|          38|
# |  Central|          15|
# |Northeast|          15|
# +---------+------------+
# only showing top 3 rows


# >>> rdd.select("REGION","MARKET").groupBy("REGION","MARKET").count().show()
# +---------+--------------------+-----+
# |   REGION|              MARKET|count|
# +---------+--------------------+-----+
# |  Central|             Chicago|12078|
# |Southeast|             Orlando| 5950|
# |Southeast|           Lafayette| 3204|
# |  Central|              Austin| 6625|
# |Southeast|           Asheville|  701|
# |Southeast|Puerto Rico-Virgi...| 7257|
# |Southeast|          Evansville| 3713|
# |Southeast|          Greenville| 2514|
# |Southeast|         Gainesville|  873|
# |Southeast|           Knoxville| 2626|
# |Northeast|         Northern MI|    1|
# |Southeast|               Tampa| 3504|
# |Southeast|       North Georgia|13769|
# |  Central|       Central Texas| 5060|
# |  Central|       Oklahoma City| 6355|
# |Southeast|       West Virginia| 2667|
# |Southeast|           Lexington| 3357|
# |Southeast|        Jacksonville| 2947|
# |Northeast|Western Pennsylvania| 6238|
# |Southeast|            Virginia| 5196|
# +---------+--------------------+-----+
# only showing top 20 rows

# get top 2 market for each region
# >>> from pyspark.sql.window import Window
# >>> grouped = rdd.select("REGION","MARKET").groupBy("REGION","MARKET").count().dropna()
# >>> window = Window.partitionBy(grouped['REGION']).orderBy(grouped['count'].desc())
# >>> grouped.select('*', sqlf.rank().over(window).alias('rank')).filter(sqlf.col('rank')<=2).show()
# +---------+----------------+-----+----+
# |   REGION|          MARKET|count|rank|
# +---------+----------------+-----+----+
# |  Unknown|         Unknown|  121|   1|
# |Southeast|   North Georgia|13769|   1|
# |Southeast|   South Florida|10885|   2|
# |     West|     Los Angeles|22487|   1|
# |     West|   San Francisco|12476|   2|
# |  Central|Dallas Ft. Worth|17638|   1|
# |  Central|         Chicago|12078|   2|
# |Northeast|   Massachusetts| 7508|   1|
# |Northeast|      Upstate NY| 6432|   2|
# |         |                |17490|   1|
# +---------+----------------+-----+----+

#range function
#https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html
def top_market_for_region(sc):
    sc = SparkContext(appName="DATA-JOIN-HDFS")
    #Set out put replication factor to 1
    #sc._jsc.hadoopConfiguration().set("dfs.replication", "1")
    #let program comunicate hdfs blocks from remote
    sc._jsc.hadoopConfiguration().set("dfs.client.use.datanode.hostname", "true")
    sqlContext = SQLContext(sc)
    rdd_dimension = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true') \
        .load("hdfs://hdfs2:8020/user/ec2-user/ERI_CELL_REGION_MARKET.csv")
    grouped = rdd_dimension.select("REGION", "MARKET").groupBy("REGION", "MARKET").count().dropna()
    window = Window.partitionBy(grouped['REGION']).orderBy(grouped['count'].desc())
    #get top market for each region
    grouped.select('*', sqlf.rank().over(window).alias('rank')).filter(sqlf.col('rank') <= 2).show()

#word count practice

# text_file = sc.textFile("hdfs://...")
#
# text_file.flatMap(lambda line: line.split())
# .map(lambda word: (word, 1))
# .reduceByKey(lambda a, b: a + b)

def page_view_practie(sc):
    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame([Row(page_id=1, user_ids=['a', 'b', 'c'], date='12152017'),
                                     Row(page_id=2, user_ids=['a', 'd', 'e'], date='12152017'),
                                     Row(page_id=2, user_ids=['a', 'b', 'c'], date='12152017')])
    print "Show data:"
    df.show()
    # +--------+-------+---------+
    # | date | page_id | user_ids |
    # +--------+-------+---------+
    # | 12152017 | 1 | [a, b, c] |
    # | 12152017 | 2 | [a, d, e] |
    # | 12152017 | 2 | [a, b, c] |
    # +--------+-------+---------+

    #method 1:
    # flatDF = df.rdd.flatMap(lambda record: [(record.date, record.page_id, id) for id in record.user_ids])\
    #     .toDF(["date", "page_id", "user_ids"])
    #method 2:
    flatDF = df.withColumn('user_ids',sqlf.explode('user_ids'))

    print "Show flatten dataframe"
    flatDF.show()
    # +--------+-------+--------+
    # | date | page_id | user_ids |
    # +--------+-------+--------+
    # | 12152017 | 1 | a |
    # | 12152017 | 1 | b |
    # | 12152017 | 1 | c |
    # | 12152017 | 2 | a |
    # | 12152017 | 2 | d |
    # | 12152017 | 2 | e |
    # | 12152017 | 2 | a |
    # | 12152017 | 2 | b |
    # | 12152017 | 2 | c |
    # +--------+-------+--------+

    #flatDF.where(flatDF.date == '12152017').groupBy("page_id").count().show()
    # +---------+-------+
    # | page_id | count |
    # +---------+-------+
    # |       1 |     3 |
    # |       2 |     6 |
    # +---------+-------+

    print "Top viewd page_id:"
    flatDF.where(flatDF.date == '12152017')\
        .groupBy("page_id")\
        .agg(sqlf.countDistinct("user_ids").alias('USERS_COUNT'))\
        .orderBy(sqlf.desc('USERS_COUNT')).show()

    # +-------+-----------+
    # | page_id | USERS_COUNT |
    # +-------+-----------+
    # | 2 | 5 |
    # | 1 | 3 |
    # +-------+-----------+

    print "users with Top page_views count:"
    flatDF.where(flatDF.date == '12152017')\
    .groupBy("user_ids")\
    .agg(sqlf.countDistinct("page_id").alias('PAGE_COUNT'))\
    .orderBy(sqlf.desc('PAGE_COUNT')).show()
    # +--------+----------+
    # | user_ids | PAGE_COUNT |
    # +--------+----------+
    # | c | 2 |
    # | a | 2 |
    # | b | 2 |
    # | d | 1 |
    # | e | 1 |
    # +--------+----------+
    #
    # >>>

from pyspark import SparkConf, sql
#
def timewindow_practice():
    sparkConf = SparkConf().setAppName("ALU Application").setMaster("local[*]")
    sparkSession = sql.SparkSession \
        .builder \
        .config(conf=sparkConf) \
        .getOrCreate()
    stockDF = sparkSession.read.csv('AAPL.csv', header=True)
    stock2016 = stockDF.filter("year(Date)==2016")
    tumblingWindowDS = stock2016.groupBy(sqlf.window(stock2016.Date, "1 week")).agg(sqlf.avg("Close").alias("weekly_average"))
    #tumblingWindowDS.orderBy('window').show()
    tumblingWindowDS.sort('window.start')\
    .select("window.start", "window.end", "weekly_average").show(truncate = False)

def timewindow_start_time_practice():
    sparkConf = SparkConf().setAppName("ALU Application").setMaster("local[*]")
    sparkSession = sql.SparkSession \
        .builder \
        .config(conf=sparkConf) \
        .getOrCreate()
    stockDF = sparkSession.read.csv('AAPL.csv', header=True)
    #print stockDF.dtypes
    stock2016 = stockDF.filter("year(Date)==2016")
    #stock2016.agg(sqlf.avg("Close").alias("all_average")).show()
    #stock2016 = stock2016.withColumn('2_Close',stock2016["Close"] *2)
    #stock2016.show()
    tumblingWindowDS = stock2016.groupBy(sqlf.window(stock2016.Date, "1 week", "1 week", "4 days")).agg(sqlf.avg("Close").alias("weekly_average"))
    #print tumblingWindowDS.dtypes
    #tumblingWindowDS.orderBy('window').show()
    tumblingWindowDS.sort('window.start')\
    .select("window.start", "window.end", "weekly_average").show(truncate = False)


#You have a file that contains 200 billion URLs. How will you find the first unique URL?
def find_unique_urls(sc):
    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame([Row(id=1, url='url_a'),
                                     Row(id=2, url='url_b'),
                                     Row(id=3, url='url_b'),
                                     Row(id=4, url='url_c'),
                                     Row(id=5, url='url_d')])
    df = df.groupBy(df.url)\
        .agg(sqlf.count(df.url).alias('url_COUNT'), sqlf.min(df.id).alias('url_min_id'))

# +-----+---------+----------+
# |  url|url_COUNT|url_min_id|
# +-----+---------+----------+
# |url_b|        2|         2|
# |url_c|        1|         4|
# |url_d|        1|         5|
# |url_a|        1|         1|
# +-----+---------+----------+
    df.filter(df.url_COUNT==1).orderBy(sqlf.asc(df.url_min_id)).show(1)

if __name__ == "__main__":
    #timewindow_practice()
    #timewindow_start_time_practice()

    #exit()
    sc = SparkContext(appName="BEST-PRACTICE")
    #page_view_practie(sc)
    find_unique_urls(sc)
    #sc.stop()
