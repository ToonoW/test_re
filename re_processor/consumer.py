#!/usr/bin/env python
# coding=utf-8

import json, time

from pika import (
    BlockingConnection,
    URLParameters,
    BasicProperties)
from pika.exceptions import AMQPConnectionError

from re_processor import settings
from re_processor.common import debug_logger as logger


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

    def consume(self, ch, method, properties, body):
        log = {'ts': time.time()}
        try:
            self.process(body, log)
        except Exception, e:
            logger.exception(e)
            log['exception'] = str(e)
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        log['proc_t'] = int((time.time() - log['ts']) * 1000)
        logger.info(json.dumps(log))

    def process(self, body, biz_log=None):
        print body

    def start(self):
        self.channel.queue_declare(queue=self.queue, durable=True)
        self.channel.queue_bind(
            exchange=settings.EXCHANGE,
            queue=self.queue,
            routing_key=self.queue)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.consume, queue=self.queue)
        self.channel.start_consuming()

    def publish(self, routing_key, message):
        log = {'module': 're_processor',
                   'ts': time.time(),
                   'topic': routing_key}
        self.channel.basic_publish(settings.EXCHANGE, routing_key,
                                   json.dumps(message),
                                   properties=BasicProperties(delivery_mode=2))
        log['proc_t'] = int((time.time() - log['ts']) * 1000)
        logger.info(json.dumps(log))