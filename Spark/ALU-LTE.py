#submit this job
#$SPARK_HOME/bin/spark-submit --master spark://ec2-34-208-33-205.us-west-2.compute.amazonaws.com:7077 --deploy-mode cluster --executor-memory 1g ALU-LTE.py
import pandas as pds
import datetime as dt
import glob
import os
import s3fs
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
    currentDirectory = os.path.dirname(__file__)
    #intDirectory = os.path.join(os.path.dirname(__file__), '..', '..', 'TestAnalysis/output-alu-new/')
    #outDirectory = os.path.join(os.path.dirname(__file__), '..', '..', 'TestAnalysis/Report/')
    intDirectory = os.path.join(os.path.dirname(__file__), 'sample-data/')
    outDirectory = os.path.join(os.path.dirname(__file__), 'report/')
    groupby = 'MARKET'

    def run(self, inputType, s3bucket):
        outputName = self.outDirectory + "network_capacity_trending_per_wk_" + self.groupby + "_ALU_2017_pandas_" + inputType + ".csv"
        if not os.path.exists(self.outDirectory):
            os.mkdir(self.outDirectory)
        fileList = glob.glob(self.intDirectory + '*.csv')
        if inputType == 's3':
            fs = s3fs.S3FileSystem(anon=False,
                                   key='AKIAI6B4YJZUYKPRSF3Q',
                                   secret='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')
            fileList = fs.ls(s3bucket)
        start = dt.datetime.now()

        datalist = []
        for filename in fileList:
            if inputType == 's3':
                filename = "s3://"+filename
            df = pds.read_csv(filename, index_col=False, usecols=[0, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])
            date = LTE_MAPPING.x_date(filename[len(filename) - 12:len(filename) - 4])
            df['DATE'] = date
            datalist.append(df)
        print "reading finished!"
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
        dataframeoutput.to_csv(outputName,
                               columns=['VENDOR', 'BAND', 'DRBPDCPSDUKBYTESDL_NONGBR', 'Cell Used PRB', 'Ave_SE',
                                        'UE Traffic (kbytes)', 'EUCELL_DL_TPUT_DEN_SECS', 'Ave_UE_Tput', 'TOTALCELLS',
                                        'TOTALSPECTRUM'],
                               header=['VENDOR', 'BAND', 'Cell Traffic (kbytes)', 'Cell Used PRB',
                                       'Cell Spectral Efficiency (bps/Hz)', 'UE Traffic (kbytes)', 'UE Active Time (s)',
                                       'UE Tput (kbps)', 'Total cell count', 'Total Spectrum in MHz'])
        difference = dt.datetime.now() - start
        print 'Ok'
        return difference

class ALU_LTE_SPARK(object):
    currentDirectory = os.path.dirname(__file__)
    #intDirectory = os.path.join(os.path.dirname(__file__), '..', '..', 'TestAnalysis/output-alu-new/')
    #outDirectory = os.path.join(os.path.dirname(__file__), '..', '..', 'TestAnalysis/Report/')
    intDirectory = os.path.join(os.path.dirname(__file__), 'sample-data/')
    outDirectory = os.path.join(os.path.dirname(__file__), 'report/')
    groupby = 'MARKET'
    dataframeoutput = None

    def printDfPartitions(self, rdddataframe):
        print "We have total " + str(rdddataframe.rdd.getNumPartitions()) + " partions!"

    def getBandRDDandUnion(self, rdddataframe, option):
        dataframe1 = rdddataframe.filter(rdddataframe['BAND'] == option).groupBy(['DATE', self.groupby]).sum()
        dataframe1 = dataframe1.withColumn('UE Tput (kbps)', dataframe1['sum(EUCELL_DL_TPUT_NUM_KBITS)'] / dataframe1[
            'sum(EUCELL_DL_TPUT_DEN_SECS)'])
        dataframe1 = dataframe1.withColumn('DRB Tput (kbps)',
                                           dataframe1['sum(EUCELL_DL_DRB_TPUT_NUM_KBITS)'] / dataframe1[
                                               'sum(EUCELL_DL_DRB_TPUT_DEN_SECS)'])
        dataframe1 = dataframe1.withColumn('Cell Spectral Efficiency (bps/Hz)',
                                           8 * dataframe1['sum(DRBPDCPSDUKBYTESDL_NONGBR)'] / (
                                               dataframe1['sum(DLPRBUSEDWITHDSPUC_FDUSERS)'] + dataframe1[
                                                   'sum(DLPRBUSEDWITHDSPUC_FSUSERS)']) / 1.024 / 0.18)
        dataframe1 = dataframe1.withColumn('BAND', sql.functions.lit(option))
        dataframe1 = dataframe1.withColumn('VENDOR', sql.functions.lit('ALU'))
        if self.dataframeoutput == None:
            self.dataframeoutput = dataframe1
        else:
            self.dataframeoutput = self.dataframeoutput.union(dataframe1)
        print option
        return option
        #return dataframe1
    def run(self, inputType, s3bucket):
        sparkConf = SparkConf().setAppName("ALU Application").setMaster("local[*]")\
            #.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sparkSession = sql.SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
        outputName = self.outDirectory + "network_capacity_trending_per_wk_" + self.groupby + "_ALU_2017_spark_" + inputType + ".csv"
        if not os.path.exists(self.outDirectory):
            os.mkdir(self.outDirectory)
        fileList = glob.glob(self.intDirectory + '*.csv')
        if inputType == 's3': #get fileList in S3
            fs = s3fs.S3FileSystem(anon=False,
                                   key='AKIAI6B4YJZUYKPRSF3Q',
                                   secret='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')
            fileList = fs.ls(s3bucket)
            #sparkSession.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIAI6B4YJZUYKPRSF3Q")
            #sparkSession.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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")
            sparkSession.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAI6B4YJZUYKPRSF3Q")
            sparkSession.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
        start = dt.datetime.now()

        dataframe = None
        for filename in fileList:
            date = LTE_MAPPING.x_date(filename[len(filename) - 12:len(filename) - 4])
            if inputType == 's3':
                #filename = "s3n://"+filename
                filename = "s3a://" + filename
            print date
            rddFrame1 = sparkSession.read.csv(filename,header=True)
            #ENODEB_CELLNAME	ENODEB	DATA_DATE	MARKET_CLUSTER	VERSION	REGION	MARKET	DL_CH_BANDWIDTH	EARFCN_DL	DRBPDCPSDUKBYTESDL_NONGBR	DLPRBUSEDWITHDSPUC_FDUSERS	DLPRBUSEDWITHDSPUC_FSUSERS	EUCELL_DL_TPUT_NUM_KBITS	EUCELL_DL_TPUT_DEN_SECS	EUCELL_DL_DRB_TPUT_NUM_KBITS	EUCELL_DL_DRB_TPUT_DEN_SECS
            #rddFrame1 = rddFrame1.drop('ENODEB','DATA_DATE','VERSION').dropna()
            rddFrame1 = rddFrame1.dropna()
            rddFrame1 = rddFrame1.withColumn('DATE', sql.functions.lit(date))
            if dataframe == None:
                dataframe = rddFrame1
            else:
                dataframe = dataframe.union(rddFrame1)
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

        #dataframe.cache()

        self.dataframeoutput = dataframe.groupBy(['DATE', self.groupby, 'BAND']).sum()
        self.dataframeoutput = self.dataframeoutput.withColumn('UE Tput (kbps)', self.dataframeoutput['sum(EUCELL_DL_TPUT_NUM_KBITS)'] / self.dataframeoutput['sum(EUCELL_DL_TPUT_DEN_SECS)'])
        self.dataframeoutput = self.dataframeoutput.withColumn('DRB Tput (kbps)', self.dataframeoutput['sum(EUCELL_DL_DRB_TPUT_NUM_KBITS)'] / self.dataframeoutput['sum(EUCELL_DL_DRB_TPUT_DEN_SECS)'])
        self.dataframeoutput = self.dataframeoutput.withColumn('Cell Spectral Efficiency (bps/Hz)', 8 * self.dataframeoutput['sum(DRBPDCPSDUKBYTESDL_NONGBR)'] / (
            self.dataframeoutput['sum(DLPRBUSEDWITHDSPUC_FDUSERS)'] + self.dataframeoutput['sum(DLPRBUSEDWITHDSPUC_FSUSERS)']) / 1.024 / 0.18)
        self.dataframeoutput = self.dataframeoutput.withColumn('VENDOR', sql.functions.lit('ALU'))
        self.dataframeoutput = self.dataframeoutput.withColumn('UE Traffic (kbytes)', self.dataframeoutput['sum(EUCELL_DL_TPUT_NUM_KBITS)'] / 8)
        self.dataframeoutput = self.dataframeoutput.withColumn('Cell Used PRB', (self.dataframeoutput['sum(DLPRBUSEDWITHDSPUC_FDUSERS)'] + self.dataframeoutput[
            'sum(DLPRBUSEDWITHDSPUC_FSUSERS)']) * 1.024)
        #rename colname
        self.dataframeoutput = self.dataframeoutput.withColumnRenamed("sum(DRBPDCPSDUKBYTESDL_NONGBR)", "Cell Traffic (kbytes)")
        self.dataframeoutput = self.dataframeoutput.withColumnRenamed("sum(EUCELL_DL_TPUT_DEN_SECS)", "UE Active Time (s)")
        self.dataframeoutput = self.dataframeoutput.withColumnRenamed("sum(Total cell count)", "Total cell count")
        self.dataframeoutput = self.dataframeoutput.withColumnRenamed("sum(Total Spectrum in MHz)", "Total Spectrum in MHz")
        #dataframeoutput = dataframeoutput.drop('sum(EUCELL_DL_TPUT_NUM_KBITS)').drop('sum(DLPRBUSEDWITHDSPUC_FDUSERS)').drop('sum(DLPRBUSEDWITHDSPUC_FSUSERS)').drop('sum(EUCELL_DL_DRB_TPUT_NUM_KBITS)').drop('sum(EUCELL_DL_DRB_TPUT_DEN_SECS)')
        self.dataframeoutput = self.dataframeoutput.select('DATE','MARKET','VENDOR','BAND','Cell Traffic (kbytes)', 'Cell Used PRB', 'Cell Spectral Efficiency (bps/Hz)',
                                                 'UE Traffic (kbytes)', 'UE Active Time (s)',
                                                 'UE Tput (kbps)', 'Total cell count', 'Total Spectrum in MHz')
        self.dataframeoutput = self.dataframeoutput.coalesce(1)
        #take action here
        self.dataframeoutput.write.csv(outputName, header=True)
        difference = dt.datetime.now() - start
        self.dataframeoutput.unpersist()
        sparkSession.stop()
        print 'Ok'
        return difference


if __name__ == "__main__":
    inputType = "local"
    #inputType = "s3"
    s3bucket = "output-alu-new"
    #print ALU_LTE_PANDAS().run(inputType, s3bucket)
    print ALU_LTE_SPARK().run(inputType, s3bucket)