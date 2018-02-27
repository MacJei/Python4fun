#sudo cp downloads/spark-streaming-kafka-0-8-assembly_2.11-2.2.0.jar /Library/Python/2.7/site-packages/pyspark/jars/.
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer #kafka api http://kafka-python.readthedocs.io/en/master/
from kafka.errors import KafkaError, KafkaTimeoutError
import logging
logging.basicConfig()
logger = logging.getLogger('spark-streaming')
logger.setLevel(logging.DEBUG)
import atexit
import json
import time

def SPARK_STREAMING_LOGIC(stream,kafka_producer):
    def send_to_kafka(rdd):
        results = rdd.collect()
        for r in results:
            data = json.dumps(
                {
                    'symbol': r[0],
                    'timestamp': time.time(),
                    'average': r[1]
                }
            )
            try:
                logger.info('Sending average price %s to kafka' % data)
                kafka_producer.send('testOutput1', value=data)
            except KafkaError as error:
                logger.warn('Failed to send average stock price to kafka, caused by: %s', error.message)
    def pair(data):
        record = json.loads(data[1].decode('utf-8'))[0]
        # GOOG -> (70,1)
        # GOOG -> (70,1)
        # AAPL -> (20,1)
        # AAPL -> (30,1)
        return record.get('symbol'), (float(record.get('price')), 1)

    # GOOG -> (150, 2)
    # AAPL -> (50, 2)

    # GOOG -> 75
    # AAPL -> 25
    stream.map(pair).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda (k, v): (k, v[0]/v[1])).foreachRDD(send_to_kafka)


def shutdown_hook(producer):
    """
    a shutdown hook to be called before the shutdown
    :param producer: instance of a kafka producer
    :return: None
    """
    try:
        logger.info('Flushing pending messages to kafka, timeout is set to 10s')
        producer.flush(10)
        logger.info('Finish flushing pending messages to kafka')
    except KafkaError as kafka_error:
        logger.warn('Failed to flush pending messages to kafka, caused by: %s', kafka_error.message)
    finally:
        try:
            logger.info('Closing kafka connection')
            producer.close(10)
        except Exception as e:
            logger.warn('Failed to close kafka connection, caused by: %s', e.message)




if __name__ == "__main__":
    BrokerURL = "node1:9092,node2:9092,node3:9092"
    sc = SparkContext("local[*]", "spark-streaming")
    sc.setLogLevel('WARN')
    streamingContext = StreamingContext(sc,1)
    dStream = KafkaUtils.createDirectStream(streamingContext,['bigdata'],{'metadata.broker.list':BrokerURL})
    kafka_producer = KafkaProducer(bootstrap_servers=BrokerURL)
    SPARK_STREAMING_LOGIC(dStream,kafka_producer)
    atexit.register(shutdown_hook,kafka_producer)
    streamingContext.start()
    streamingContext.awaitTermination()