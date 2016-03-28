#!/usr/bin/env python
# coding=utf-8

import time, json, operator
from collections import defaultdict

from pika import (
    BlockingConnection,
    URLParameters,
    BasicProperties)
from pika.exceptions import AMQPConnectionError

from re_processor import settings
from re_processor.connections import get_mysql, get_redis
from re_processor.common import debug_logger as logger


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

    def mq_subcribe(self, queue, product_key):
        self.channel.queue_declare(queue=queue, durable=True)
        self.channel.queue_bind(
            exchange=settings.EXCHANGE,
            queue=queue,
            routing_key=settings.ROUTING_KEY[queue].format(product_key))

    def consume(self, ch, method, properties, body):
        log = {'ts': time.time()}
        try:
            #print body
            lst = self.unpack(body, log)
            if lst:
                msg = map(lambda m: self.process_msg(m, log), lst)
                if msg:
                    self.send(reduce(operator.__add__, msg), log)
        except Exception, e:
            logger.exception(e)
            log['exception'] = str(e)
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        log['proc_t'] = int((time.time() - log['ts']) * 1000)
        logger.info(json.dumps(log))

    def mq_listen(self, queue, product_key):
        self.mq_subcribe(queue, product_key)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.consume, queue=queue)
        self.channel.start_consuming()

    def mq_publish(self, product_key, msg_list):
        for msg in msg_list:
            routing_key = settings.PUBLISH_ROUTING_KEY[msg['action_type']]
            log = {
                'module': 're_processor',
                'action': 'pub',
                'ts': time.time(),
                'topic': routing_key
            }
            self.channel.basic_publish(settings.EXCHANGE, routing_key,
                                       json.dumps(msg),
                                       properties=BasicProperties(delivery_mode=2))
            log['proc_t'] = int((time.time() - log['ts']) * 1000)
            logger.info(json.dumps(log))

    def mq_unpack(self, body, log=None):
        msg = json.loads(body)
        if msg.has_key('data'):
            data = msg.pop('data')
            if 'attr_fault' == msg['event_type'] or 'attr_alert' == msg['event_type']:
                msg[data['attr_name']] = data['value']
                msg['attr_displayname'] = data['attr_displayname']
            else:
                msg.update(data)

        event =  settings.TOPIC_MAP[msg['event_type']]
        msg['sys.time_now'] = int(time.time())

        if 'online' == event:
            msg['online.status'] = True
            msg['offline.status'] = False
        elif 'offline' == event:
            msg['online.status'] = False
            msg['offline.status'] = True
        elif 'bind' == event:
            msg['bind.status'] = True
            msg['unbind.status'] = False
        elif 'unbind' == event:
            msg['bind.status'] = False
            msg['unbind.status'] = True


        db = get_mysql()
        sql = 'select `id`, `rule_tree`, `custom_vars` from `{0}` where `obj_id`="{1}" or `obj_id`="{2}"'.format(
            settings.MYSQL_TABLE['rule']['table'],
            msg['did'],
            msg['product_key'])
        db.execute(sql)
        msg_list = []
        for rule_id, rule_tree, custom_vars in db.fetchall():
            rule_tree = json.loads(rule_tree) if rule_tree else []
            custom_vars = json.loads(custom_vars) if custom_vars else {}
            __rule_tree_list = [{'event': msg['event_type'],
                               'rule_id': rule_id,
                               'msg_to': settings.MSG_TO['internal'],
                               'ts': log['ts'],
                               'current': x['task_list'][0][0],
                               'task_list': x['task_list'],
                               'task_vars': msg,
                               'custom_vars': custom_vars}
                              for x in rule_tree if event == x['event'] and x['task_list']]
            if __rule_tree_list:
                msg_list.extend(__rule_tree_list)

        db.close()

        return msg_list


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
                    print '{} IDLE-----sleep 1s'.format(self.queue)
                    continue
                #print msg
                lst = self.unpack(msg[1], log)
                msg = self.process_msg(lst, log)
                if not msg:
                    continue
                self.send(msg, log)
            except Exception, e:
                logger.exception(e)
                log['exception'] = str(e)
            log['proc_t'] = int((time.time() - log['ts']) * 1000)
            logger.info(json.dumps(log))

    def redis_publish(self, product_key, msg_list):
        msg_dict = defaultdict(list)
        for msg in msg_list:
            msg_dict[msg['current']].append(msg)
        for key, val in msg_dict.items():
            self.redis_conn.lpush('rules_engine.{0}.{1}'.format(key, product_key), *map(json.dumps, val))

    def redis_unpack(self, body, log=None):
        return json.loads(body)


class CommonTransceiver(object):
    '''
    mixins transceiver
    '''

    def send(self, body, log=None):
        for _type, method in settings.TRANSCEIVER['send'].items():
            msg_list = filter(lambda x: _type == x['msg_to'], body)
            if msg_list:
                getattr(self, method)(self.product_key, msg_list)

    def unpack(self, body, log=None):
        return getattr(self, self.unpack_method)(body, log)

    def begin(self):
        self.unpack_method = settings.TRANSCEIVER['unpack'][self.receiver_type]
        self.begin_method = settings.TRANSCEIVER['begin'][self.receiver_type]
        getattr(self, self.begin_method)(self.queue, self.product_key)

class InternalTransceiver(CommonTransceiver):
    '''
    mixins transceiver
    '''

    def init_queue(self):
        self.receiver_type = settings.MSG_TO['internal']
        self.redis_initial(self.queue, self.product_key)


class MainTransceiver(CommonTransceiver):
    '''
    mixins transceiver
    '''

    def init_queue(self):
        self.receiver_type = settings.MSG_TO['external']
        self.mq_initial(self.queue, self.product_key)
        self.redis_initial(self.queue, self.product_key)


class OutputTransceiver(CommonTransceiver):
    '''
    mixins transceiver
    '''

    def init_queue(self):
        self.receiver_type = settings.MSG_TO['internal']
        self.mq_initial(self.queue, self.product_key)
        self.redis_initial(self.queue, self.product_key)
