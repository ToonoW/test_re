#!/usr/bin/env python
# coding=utf-8

import time, json

from pika import (
    BlockingConnection,
    URLParameters,
    PlainCredentials,
    BasicProperties)
from pika.exceptions import AMQPConnectionError

import settings

import logging

logger = logging.getLogger('pocessor')
biz_logger = logging.getLogger('biz_logger')


class BaseRabbitmqConsumer(object):

    def __init__(self, queue):
        # connect rabbitmq
        self.queue = queue
        try:
            self.m2m_conn = BlockingConnection(
                URLParameters(settings.M2M_MQ_URL))
        except AMQPConnectionError, e:
            logger.exception(e)
            exit(1)
        self.channel = self.m2m_conn.channel()

        # connect mongo
        try:
            self.db = utils.get_mongodb('gizwits_data')
        except Exception, e:
            logger.exception(e)
            exit(1)

    def consume(self, ch, method, properties, body):
        biz_log = {'ts': time.time()}
        try:
            self.process(body, biz_log)
        except Exception, e:
            logger.exception(e)
            biz_log['exception'] = str(e)
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        biz_log['proc_t'] = int((time.time() - biz_log['ts']) * 1000)
        biz_logger.info(json.dumps(biz_log))

    def process(self, body, biz_log=None):
        pass

    def start(self):
        self.channel.queue_declare(queue=self.queue, durable=True)
        self.channel.queue_bind(
            exchange=settings.EXCHANGE,
            queue=self.queue,
            routing_key=QUEUES[self.queue]['routing_key'])
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.consume, queue=self.queue)
        self.channel.start_consuming()

    def publish(self, routing_key, message, exchange='amq.topic'):
        biz_log = {'module': 're_processor',
                   'action': 'pub',
                   'ts': time.time(),
                   'topic': routing_key}
        self.channel.basic_publish(exchange, routing_key,
                                   json.dumps(message),
                                   properties=BasicProperties(delivery_mode=2))
        biz_log['proc_t'] = int((time.time() - biz_log['ts']) * 1000)
        biz_logger.info(json.dumps(biz_log))
