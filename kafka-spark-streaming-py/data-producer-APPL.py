import json
import argparse
import schedule
import logging
import time
import atexit
from kafka import KafkaProducer
import random

logging.basicConfig()
logger = logging.getLogger('data-producer')

#debug,info,warn,error,fatal
logger.setLevel(logging.DEBUG)

def shutdown_hook(producer):
    logger.info('closing kafka producer')
    producer.flush(10)
    producer.close(10)
    logger.info('kafka producer closed')

def fetch_price_and_send(producer):
    logger.debug('about to fetch price')
    trade_time = int(round(time.time()*1000))
    price = random.randint(30,120)
    data = {
        'symbol': symbol,
        'last_trade_time':trade_time,
        'price': price
    }
    data = json.dumps(data)
    logger.info('retrieved stock price %s', data)
    try:
        producer.send(topic=topic_name, value=data)
        logger.debug('sent data to kafka %s', data)
    except Exception as e:
        logger.warn('failed to send price to kafka')

if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('symbol', help='the symbol of the stock')
    # parser.add_argument('topic_name',help='the name of the stock')
    # parser.add_argument('kafka_broker',help='the location of the stock')
    #
    # args = parser.parse_args()
    # symbol = args.symbol
    # topic_name = args.topic_name
    # kafka_broker = args.kafka_broker

    symbol = 'APPL'
    topic_name = 'bigdata'
    kafka_broker = 'node1:9092,node2:9092,node3:9092'

    producer = KafkaProducer(
        bootstrap_servers=kafka_broker
    )

    schedule.every(1).second.do(fetch_price_and_send, producer)

    atexit.register(shutdown_hook, producer)

    while True:
        schedule.run_pending()
        time.sleep(1)