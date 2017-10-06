#pyspark 2.2

import pandas as pds
import datetime as dt
import glob
import os
import s3fs
import hdfs
from pyspark import SparkContext, SparkConf, sql

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

class ALU_LTE_PANDAS(object):

    groupby = 'MARKET'

    def run(self, inputType, fileList, outDirectory):
        outputName = outDirectory + "result_group_by_" + self.groupby + "_ALU_2017_pandas_" + inputType + ".csv"
        start = dt.datetime.now()
        datalist = []
        print 'Pandas starts reading ' + inputType + ' data source:'
        for filename in fileList:
            if inputType == 's3':
                filename = "s3a://"+filename
            print "Reading " + filename
            df = pds.read_csv(filename, index_col=False, usecols=[0, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])
            date = LTE_MAPPING.x_date(filename[len(filename) - 12:len(filename) - 4])
            df['DATE'] = date
            datalist.append(df)
        print "Reading finished, starts our data analysis!"
        dataframe = pds.concat(datalist, ignore_index=True)
        dataframe = dataframe.dropna(axis=0)
        dataframe['TOTALCELLS'] = 1
        dataframe['BAND'] = dataframe['EARFCN_DL'].map(lambda x: LTE_MAPPING.EARFCN_DL_mapping(x))
        dataframe['TOTALSPECTRUM'] = dataframe['DL_CH_BANDWIDTH'].map(lambda x: LTE_MAPPING.bandwidth(x))
        dataframe_list = []
        bandlist = [700, 850, 1900, 2100, 2300, 'AWS']
        for option in bandlist:
            dataframe_breakdown = dataframe[dataframe['BAND'] == option]
            dataframe1 = dataframe_breakdown.groupby(['DATE', self.groupby]).sum()
            try:
                dataframe1['Ave_UE_Tput'] = dataframe1['EUCELL_DL_TPUT_NUM_KBITS'] / dataframe1[
                    'EUCELL_DL_TPUT_DEN_SECS']
            except ValueError:
                dataframe1['Ave_UE_Tput'] = 0
            dataframe1['Ave_DRB_Tput'] = dataframe1['EUCELL_DL_DRB_TPUT_NUM_KBITS'] / dataframe1[
                'EUCELL_DL_DRB_TPUT_DEN_SECS']
            try:
                dataframe1['Ave_SE'] = 8 * dataframe1['DRBPDCPSDUKBYTESDL_NONGBR'] / (
                    dataframe1['DLPRBUSEDWITHDSPUC_FDUSERS'] + dataframe1['DLPRBUSEDWITHDSPUC_FSUSERS']) / 1.024 / 0.18
            except ValueError:
                dataframe1['Ave_SE'] = 0
            dataframe1['BAND'] = option
            dataframe1['VENDOR'] = "ALU"
            dataframe_list.append(dataframe1)
        dataframeoutput = pds.concat(dataframe_list, ignore_index=False)

        dataframeoutput['UE Traffic (kbytes)'] = dataframeoutput['EUCELL_DL_TPUT_NUM_KBITS'] / 8
        dataframeoutput['Cell Used PRB'] = (dataframeoutput['DLPRBUSEDWITHDSPUC_FDUSERS'] + dataframeoutput[
            'DLPRBUSEDWITHDSPUC_FSUSERS']) * 1.024
        print "Writing output!"
        dataframeoutput.to_csv(outputName,
                               columns=['VENDOR', 'BAND', 'DRBPDCPSDUKBYTESDL_NONGBR', 'Cell Used PRB', 'Ave_SE',
                                        'UE Traffic (kbytes)', 'EUCELL_DL_TPUT_DEN_SECS', 'Ave_UE_Tput', 'TOTALCELLS',
                                        'TOTALSPECTRUM'],
                               header=['VENDOR', 'BAND', 'Cell Traffic (kbytes)', 'Cell Used PRB',
                                       'Cell Spectral Efficiency (bps/Hz)', 'UE Traffic (kbytes)', 'UE Active Time (s)',
                                       'UE Tput (kbps)', 'Total cell count', 'Total Spectrum in MHz'])
        difference = dt.datetime.now() - start
        print 'Done, total running time for ' + inputType + ' data source is :'
        return difference

class ALU_LTE_SPARK(object):

    groupby = 'MARKET'

    def printDfPartitions(self, rdddataframe):
        print "We have total " + str(rdddataframe.rdd.getNumPartitions()) + " partions!"

    def run(self, inputType, fileList, outDirectory):
        sparkConf = SparkConf().setAppName("ALU Application").setMaster("local[*]")
        sparkSession = sql.SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
        outputName = outDirectory + "result_group_by_" + self.groupby + "_ALU_2017_spark_" + inputType + ".csv"
        start = dt.datetime.now()
        dataframe = None
        print 'Spark starts reading ' + inputType + ' data source:'
        for filename in fileList:
            date = LTE_MAPPING.x_date(filename[len(filename) - 12:len(filename) - 4])
            if inputType == 'hdfs':
                filename = "hdfs://hdfs1:8020/user/ec2-user/sample-data/" + filename
                #The following setup is important if you are running this program in your local desktop
                #Pls first set up DNS in local environment's /etc/hosts
                sparkSession.sparkContext._jsc.hadoopConfiguration().set("dfs.client.use.datanode.hostname", "true")
            if inputType == 's3':
                filename = "s3a://" + filename
                # for spark, you can set aws credential via
                # (1) export env for your spark cluster:
                # export AWS_SECRET_ACCESS_KEY=XXXXXXXXXX
                # export AWS_ACCESS_KEY_ID=XXXXXXXXXXXXXXXXXX
                # (2) or you can explicitly set credential at runtime:
                # s3n works but it is very slow
                # sparkSession.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", access_key)
                # sparkSession.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", secret_key)
                # s3a is faster!
                # sparkSession.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
                # sparkSession.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
            print "Reading " + filename
            rddFrame1 = sparkSession.read.csv(filename,header=True)
            #ENODEB_CELLNAME	ENODEB	DATA_DATE	MARKET_CLUSTER	VERSION	REGION	MARKET	DL_CH_BANDWIDTH	EARFCN_DL	DRBPDCPSDUKBYTESDL_NONGBR	DLPRBUSEDWITHDSPUC_FDUSERS	DLPRBUSEDWITHDSPUC_FSUSERS	EUCELL_DL_TPUT_NUM_KBITS	EUCELL_DL_TPUT_DEN_SECS	EUCELL_DL_DRB_TPUT_NUM_KBITS	EUCELL_DL_DRB_TPUT_DEN_SECS
            #rddFrame1 = rddFrame1.drop('ENODEB','DATA_DATE','VERSION').dropna()
            rddFrame1 = rddFrame1.dropna()
            rddFrame1 = rddFrame1.withColumn('DATE', sql.functions.lit(date))
            if dataframe == None:
                dataframe = rddFrame1
            else:
                dataframe = dataframe.union(rddFrame1)
        print "Reading finished, starts our data analysis!"
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
        print "Writing output!"
        dataframeoutput.write.csv(outputName, header=True)
        difference = dt.datetime.now() - start
        dataframeoutput.unpersist()
        sparkSession.stop()
        print 'Done, total running time for ' + inputType +' data source is :'
        return difference

if __name__ == "__main__":
    intDirectory = os.path.join(os.path.dirname(__file__), 'sample-data/')
    outDirectory = os.path.join(os.path.dirname(__file__), 'report/')
    s3bucket = "output-alu-new" # your s3bucket name
    s3Files = s3fs.S3FileSystem(anon=False).ls(s3bucket)
    localFiles = glob.glob(intDirectory + '*.csv')
    # Pls first set up DNS in local environment's /etc/hosts     XXX.XXX.XXX hdfs1
    hdfsFiles = hdfs.Client('http://hdfs1:50070').list('/user/ec2-user/sample-data') # Use namenode public ip http://namenode:50070
    #(1) run via pandas
    print ALU_LTE_PANDAS().run("local", localFiles, outDirectory)
    print ALU_LTE_PANDAS().run("s3", s3Files, outDirectory)
    #(2) run via spark
    print ALU_LTE_SPARK().run("local", localFiles, outDirectory)
    print ALU_LTE_SPARK().run("s3", s3Files, outDirectory)
    print ALU_LTE_SPARK().run("hdfs", hdfsFiles, outDirectory)
    exit()
    # if you already update your local aws credentials by vi ~/.aws/credentials
    # then you don't need to explicitly set the access_key, secret_key
    # otherwise, we have to set aws credentials at runtime
    # fs = s3fs.S3FileSystem(anon=False, key=access_key, secret=secret_key)

#python spark-pandas-hdfs-s3.py

#or

#spark-submit to your spark stand-alone cluster
#$SPARK_HOME/bin/spark-submit --master spark://node1:7077 --deploy-mode cluster --executor-memory 1g spark-pandas-hdfs-s3.py
