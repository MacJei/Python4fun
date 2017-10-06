#spark 1.6
import datetime as dt
import os
import hdfs
from pyspark import SparkContext, sql

class LTE_MAPPING(object):
    @staticmethod
    def print_record(x):
        print x
    @staticmethod
    def x_date(x):
        datetime1 = dt.datetime.strptime(str(x), '%Y%m%d')
        date1 = datetime1.strftime('%m/%d/%Y')
        return date1

    @staticmethod
    def EARFCN_DL_mapping(EARFCN_DL):
        x = int(EARFCN_DL)
        if x == 412:
            return 2100
        if x >= 625 and x <= 1175:
            return 1900
        if x >= 1975 and x <= 2350:
            return 'AWS'
        if x >= 2425 and x <= 2618:
            return 850
        if x == 5035:
            return 700
        if x >= 5760 and x <= 5815:
            return 700
        if x == 9720:
            return 700
        if x == 9685:
            return 700
        if x == 9820:
            return 2300
        else:
            return 2300

    @staticmethod
    def bandwidth(x):
        bandinfo = x.split('-')
        band = bandinfo[1]
        band = band[0:len(band) - 3]
        return int(band)

class ALU_LTE_SPARK(object):

    groupby = 'MARKET'

    def printDfPartitions(self, rdddataframe):
        print "We have total " + str(rdddataframe.rdd.getNumPartitions()) + " partions!"

    def run(self, inputType, fileList, outDirectory):
        sc = SparkContext(appName="ALU Application")
        sqlContext = sql.SQLContext(sc)
        outputName = outDirectory + "result_group_by_" + self.groupby + "_ALU_2017_spark_" + inputType + ".csv"
        start = dt.datetime.now()
        dataframe = None

        for filename in fileList:
            date = LTE_MAPPING.x_date(filename[len(filename) - 12:len(filename) - 4])
            if inputType == 'hdfs':
                filename = "hdfs://hdfs1:8020/user/ec2-user/sample-data/" + filename
            print "reading " + filename
            rddFrame1 = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true')\
                .load(filename)
            #ENODEB_CELLNAME	ENODEB	DATA_DATE	MARKET_CLUSTER	VERSION	REGION	MARKET	DL_CH_BANDWIDTH	EARFCN_DL	DRBPDCPSDUKBYTESDL_NONGBR	DLPRBUSEDWITHDSPUC_FDUSERS	DLPRBUSEDWITHDSPUC_FSUSERS	EUCELL_DL_TPUT_NUM_KBITS	EUCELL_DL_TPUT_DEN_SECS	EUCELL_DL_DRB_TPUT_NUM_KBITS	EUCELL_DL_DRB_TPUT_DEN_SECS
            #rddFrame1 = rddFrame1.drop('ENODEB','DATA_DATE','VERSION').dropna()
            rddFrame1 = rddFrame1.dropna()
            rddFrame1 = rddFrame1.withColumn('DATE', sql.functions.lit(date))
            if dataframe == None:
                dataframe = rddFrame1
            else:
                dataframe = dataframe.unionAll(rddFrame1)
        print "reading finished!"
        self.printDfPartitions(dataframe)

        #cast Type
        dataframe = dataframe.withColumn('EUCELL_DL_TPUT_NUM_KBITS', dataframe['EUCELL_DL_TPUT_NUM_KBITS'].cast(sql.types.DoubleType()))
        dataframe = dataframe.withColumn('EUCELL_DL_TPUT_DEN_SECS', dataframe['EUCELL_DL_TPUT_DEN_SECS'].cast(sql.types.DoubleType()))
        dataframe = dataframe.withColumn('EUCELL_DL_DRB_TPUT_NUM_KBITS', dataframe['EUCELL_DL_DRB_TPUT_NUM_KBITS'].cast(sql.types.DoubleType()))
        dataframe = dataframe.withColumn('EUCELL_DL_DRB_TPUT_DEN_SECS', dataframe['EUCELL_DL_DRB_TPUT_DEN_SECS'].cast(sql.types.DoubleType()))
        dataframe = dataframe.withColumn('DRBPDCPSDUKBYTESDL_NONGBR', dataframe['DRBPDCPSDUKBYTESDL_NONGBR'].cast(sql.types.DoubleType()))
        dataframe = dataframe.withColumn('DLPRBUSEDWITHDSPUC_FDUSERS', dataframe['DLPRBUSEDWITHDSPUC_FDUSERS'].cast(sql.types.DoubleType()))
        dataframe = dataframe.withColumn('DLPRBUSEDWITHDSPUC_FSUSERS', dataframe['DLPRBUSEDWITHDSPUC_FSUSERS'].cast(sql.types.DoubleType()))
        #add columns
        dataframe = dataframe.withColumn('Total cell count', sql.functions.lit(1))
        BandMapping = sql.functions.udf(lambda x: LTE_MAPPING.EARFCN_DL_mapping(x), sql.types.StringType())
        dataframe = dataframe.withColumn('BAND', BandMapping('EARFCN_DL'))
        BandWidthMapping = sql.functions.udf(lambda x: LTE_MAPPING.bandwidth(x), sql.types.IntegerType())
        dataframe = dataframe.withColumn('Total Spectrum in MHz', BandWidthMapping('DL_CH_BANDWIDTH'))

        dataframeoutput = dataframe.groupBy(['DATE', self.groupby, 'BAND']).sum()
        dataframeoutput = dataframeoutput.withColumn('UE Tput (kbps)', dataframeoutput['sum(EUCELL_DL_TPUT_NUM_KBITS)'] / dataframeoutput['sum(EUCELL_DL_TPUT_DEN_SECS)'])
        dataframeoutput = dataframeoutput.withColumn('DRB Tput (kbps)', dataframeoutput['sum(EUCELL_DL_DRB_TPUT_NUM_KBITS)'] / dataframeoutput['sum(EUCELL_DL_DRB_TPUT_DEN_SECS)'])
        dataframeoutput = dataframeoutput.withColumn('Cell Spectral Efficiency (bps/Hz)', 8 * dataframeoutput['sum(DRBPDCPSDUKBYTESDL_NONGBR)'] / (
            dataframeoutput['sum(DLPRBUSEDWITHDSPUC_FDUSERS)'] + dataframeoutput['sum(DLPRBUSEDWITHDSPUC_FSUSERS)']) / 1.024 / 0.18)
        dataframeoutput = dataframeoutput.withColumn('VENDOR', sql.functions.lit('ALU'))
        dataframeoutput = dataframeoutput.withColumn('UE Traffic (kbytes)', dataframeoutput['sum(EUCELL_DL_TPUT_NUM_KBITS)'] / 8)
        dataframeoutput = dataframeoutput.withColumn('Cell Used PRB', (dataframeoutput['sum(DLPRBUSEDWITHDSPUC_FDUSERS)'] + dataframeoutput[
            'sum(DLPRBUSEDWITHDSPUC_FSUSERS)']) * 1.024)
        #rename colname
        dataframeoutput = dataframeoutput.withColumnRenamed("sum(DRBPDCPSDUKBYTESDL_NONGBR)", "Cell Traffic (kbytes)")
        dataframeoutput = dataframeoutput.withColumnRenamed("sum(EUCELL_DL_TPUT_DEN_SECS)", "UE Active Time (s)")
        dataframeoutput = dataframeoutput.withColumnRenamed("sum(Total cell count)", "Total cell count")
        dataframeoutput = dataframeoutput.withColumnRenamed("sum(Total Spectrum in MHz)", "Total Spectrum in MHz")
        #dataframeoutput = dataframeoutput.drop('sum(EUCELL_DL_TPUT_NUM_KBITS)').drop('sum(DLPRBUSEDWITHDSPUC_FDUSERS)').drop('sum(DLPRBUSEDWITHDSPUC_FSUSERS)').drop('sum(EUCELL_DL_DRB_TPUT_NUM_KBITS)').drop('sum(EUCELL_DL_DRB_TPUT_DEN_SECS)')
        dataframeoutput = dataframeoutput.select('DATE','MARKET','VENDOR','BAND','Cell Traffic (kbytes)', 'Cell Used PRB', 'Cell Spectral Efficiency (bps/Hz)',
                                                 'UE Traffic (kbytes)', 'UE Active Time (s)',
                                                 'UE Tput (kbps)', 'Total cell count', 'Total Spectrum in MHz')
        dataframeoutput = dataframeoutput.coalesce(1)
        #take action here
        dataframeoutput.write.format('com.databricks.spark.csv').save(outputName)
        difference = dt.datetime.now() - start
        dataframeoutput.unpersist()
        sc.stop()
        return difference

if __name__ == "__main__":
    outDirectory = os.path.join(os.path.dirname(__file__), 'report/')
    hdfsFiles = hdfs.Client('http://hdfs1:50070').list('/user/ec2-user/sample-data') # Use namenode public ip http://namenode:50070
    print "start"
    print ALU_LTE_SPARK().run("hdfs", hdfsFiles, outDirectory)
    print "OK"
    exit()

#Submit job to yarn on top of the hdfs cluster
#spark-submit --master yarn --deploy-mode cluster --executor-memory 1g --packages com.databricks:spark-csv_2.10:1.5.0 spark-CDH5-1.6-hdfs-yarn.py

#From hdfs, find output file at:
#hdfs://user/ec2-user/report/result_group_by_MARKET_ALU_2017_spark_hdfs.csv