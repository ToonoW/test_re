#!/usr/bin/env python
# coding=utf-8

import time, json, operator

from pika import (
    BlockingConnection,
    URLParameters,
    BasicProperties)
from pika.exceptions import AMQPConnectionError

from re_processor import settings
from re_processor.connections import get_mysql, get_redis
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
            lst = self.unpack(body, log)
            msg = map(lambda m: self.process_msg(m, log), lst)
            self.send(reduce(operator.__add__, msg), log)
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


class BaseRedismqConsumer(object):
    '''
    Base class for redismq consumer
    '''

    def redis_initial(self, queue, product_key):
        self.redis_conn = get_redis()

    def redis_listen(self, queue, product_key):
        while True:
            log = {'ts': time.time()}
            try:
                msg = self.redis_conn.brpop('rules_engine.{0}.{1}'.format(queue, product_key), settings.REDIS_BRPOP_TIMEOUT)
                if not msg:
                    continue
                lst = self.unpack(msg[1], log)
                msg = map(lambda m: self.process_msg(m, log), lst)
                self.send(reduce(operator.__add__, msg), log)
            except Exception, e:
                logger.exception(e)
                log['exception'] = str(e)
                print 'IDLE-----sleep 5s'
                time.sleep(5)
            log['proc_t'] = int((time.time() - log['ts']) * 1000)
            logger.info(json.dumps(log))

    def redis_publish(self, queue, product_key, message):
        self.redis_conn.lpush('rules_engine.{0}.{1}'.format(queue, product_key), json.dumps(message))


class RabbitmqTransmitter(object):
    '''
    mixins transmitter using rabbitmq
    '''
    transmitter_init = 'mq_initial'

    def send(self, body, log=None):
        self.mq_publish(self.queue, self.product_key, json.dumps(body))


class RabbitmqReceiver(object):
    '''
    mixins receiver using rabbitmq
    '''
    receiver_init = 'mq_initial'

    def unpack(self, body, log=None):
        msg = json.loads(body)
        if msg.has_key('data'):
            data = msg.pop('data')
            if 'attr_fault' == msg['event_type'] or 'attr_alert' == msg['event_type']:
                msg[data['attr_name']] = data['value']
                msg['attr_displayname'] = data['attr_displayname']
            else:
                msg.update(data)

        db = get_mysql()
        sql = 'select `id`, `rule_tree`, `custom_vars` from `{0}` where `obj_id`="{1}" or `obj_id`="{2}"'.format(
            settings.MYSQL_TABLE['rule']['table'],
            msg['did'],
            msg['product_key'])
        db.execute(sql)
        msg_list = []
        for rule_id, rule_tree, custom_vars in db.fetchall():
            event =  settings.TOPIC_MAP[msg['event_type']]
            rule_tree = json.loads(rule_tree) if rule_tree else rule_tree
            custom_vars = json.loads(custom_vars) if custom_vars else custom_vars
            __rule_tree_list = [{'event': msg['event_type'],
                               'ts': log['ts'],
                               'current': x['task_list'][0][0],
                               'task_list': x['task_list'],
                               'task_vars': msg, 'custom_vars': custom_vars}
                              for x in rule_tree if event == x['event'] and x['task_list']]
            msg_list.extend(__rule_tree_list)

        return msg_list

    def begin(self, queue, product_key):
        self.queue = queue
        self.product_key = product_key
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.consume, queue=queue)
        self.channel.start_consuming()


class RedisTransmitter(object):
    '''
    mixins transmitter using rabbitmq
    '''
    transmitter_init = 'redis_initial'

    def send(self, body, log=None):
        self.mq_publish(self.queue, self.product_key, json.dumps(body))


class RedisReceiver(object):
    '''
    mixins transmitter using rabbitmq
    '''
    receiver_init = 'redis_initial'

    def unpack(self, body, log=None):
        return json.loads(body)

    def begin(self, queue, product_key):
        self.queue = queue
        self.product_key = product_key
        self.redis_listen(queue, product_key)
