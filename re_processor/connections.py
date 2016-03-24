#!/usr/bin/env python
# coding=utf-8

import redis, MySQLdb, time, json

from pika import (
    BlockingConnection,
    URLParameters,
    BasicProperties)
from pika.exceptions import AMQPConnectionError

from pymongo import MongoClient
from re_processor import settings
from re_processor.common import logger


class BaseRabbitmqConsumer(object):
    '''
    Base class for rabbitmq consumer
    '''

    def mq_initial(self, queue, product_key):
        # connect rabbitmq
        try:
            self.m2m_conn = BlockingConnection(
                URLParameters(settings.M2M_MQ_URL))
        except AMQPConnectionError, e:
            logger.exception(e)
            exit(1)
        self.channel = self.m2m_conn.channel()
        self.mq_subcribe(queue, product_key)

    def mq_subcribe(self, queue, product_key):
        self.channel.queue_declare(queue=queue, durable=True)
        self.channel.queue_bind(
            exchange=settings.EXCHANGE,
            queue=queue,
            routing_key=settings.ROUTING_KEY[queue].format(product_key))

    def consume(self, ch, method, properties, body):
        log = {'ts': time.time()}
        try:
            self.unpack(body, log)
        except Exception, e:
            logger.exception(e)
            log['exception'] = str(e)
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        log['proc_t'] = int((time.time() - log['ts']) * 1000)
        logger.info(json.dumps(log))

    def mq_publish(self, queue, product_key, message):
        routing_key=settings.ROUTING_KEY[queue].format(product_key)
        log = {
            'module': 're_processor',
            'action': 'pub',
            'ts': time.time(),
            'topic': routing_key
        }
        self.channel.basic_publish(settings.EXCHANGE, routing_key,
                                   json.dumps(message),
                                   properties=BasicProperties(delivery_mode=2))
        log['proc_t'] = int((time.time() - log['ts']) * 1000)
        logger.info(json.dumps(log))


redis_pool = redis.ConnectionPool(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    password=settings.REDIS_PWD)


class BaseRedismqConsumer(object):
    '''
    Base class for redismq consumer
    '''

    def redis_initial(self, queue, product_key):
        self.redis_conn = redis.Redis(connection_pool=redis_pool)

    def redis_subcribe(self, queue, product_key):
        self.pubsub = self.redis_conn.pubsub()
        self.pubsub.subscribe('rules_engine.{0}.{1}'.format(queue, product_key))

    def redis_listen(self, queue, product_key):
        while True:
            log = {'ts': time.time()}
            try:
                msg = self.redis_conn.brpop('rules_engine.{0}.{1}'.format(queue, product_key), settings.REDIS_BRPOP_TIMEOUT)
                if msg:
                    self.unpack(msg[1])
                else:
                    continue
            except Exception, e:
                logger.exception(e)
                log['exception'] = str(e)
                print 'IDLE-----sleep 5s'
                time.sleep(5)
            log['proc_t'] = int((time.time() - log['ts']) * 1000)
            logger.info(json.dumps(log))

    def redis_publish(self, queue, product_key, message):
        self.redis_conn.lpush('rules_engine.{0}.{1}'.format(queue, product_key), json.dumps(message))


mongo_conn = MongoClient(settings.MONGO_DATABASES)

def get_mongodb():
    return mongo_conn.get_default_database()


class MysqlConnection(object):
    '''
    operate mysql
    '''
    def __init__(self):
        self.conn = MySQLdb.connect(
            host=settings.MYSQL_HOST,
            port=settings.MYSQL_PORT,
            user=settings.MYSQL_USER,
            passwd=settings.MYSQL_PWD,
            db=settings.MYSQL_DB
            )

    def __del__(self):
        self.conn.close()

mysql_conn = MysqlConnection()

def get_mysql():
    return mysql_conn.cursor()
