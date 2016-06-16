#!/usr/bin/env python
# coding=utf-8

import time, json, operator, copy
from collections import defaultdict

from pika import (
    BlockingConnection,
    URLParameters,
    BasicProperties)
from pika.exceptions import AMQPConnectionError
from gevent.queue import Empty

from re_processor import settings
from re_processor.connections import get_mysql, get_redis
from re_processor.common import debug_logger as logger, console_logger


class BaseRabbitmqConsumer(object):
    '''
    Base class for rabbitmq consumer
    '''

    def mq_initial(self):
        # connect rabbitmq
        try:
            self.m2m_conn = BlockingConnection(URLParameters(settings.M2M_MQ_URL))
        except AMQPConnectionError, e:
            console_logger.exception(e)
            exit(1)
        self.channel = self.m2m_conn.channel()

    def mq_subcribe(self, mq_queue_name, product_key):
        name = 'rules_engine_core_{}'.format(mq_queue_name)
        self.channel.queue_declare(queue=name, durable=True)
        self.channel.queue_bind(
            exchange=settings.EXCHANGE,
            queue=name,
            routing_key=settings.ROUTING_KEY[mq_queue_name].format(product_key))

    def mq_reconnect(self):
        # reconnect rabbitmq
        while True:
            try:
                time.sleep(5)
                self.m2m_conn = BlockingConnection(URLParameters(settings.M2M_MQ_URL))
            except AMQPConnectionError, e:
                console_logger.exception(e)
                continue
            else:
                self.channel = self.m2m_conn.channel()
                break

    def consume(self, ch, method, properties, body):
        log = {
            'ts': time.time(),
            'module': 're_processor_status',
            'running_status': 'beginning'
        }
        try:
            #print body
            lst = self.unpack(body, log)
            if lst:
                msg = map(lambda m: self.process_msg(m, log), lst)
                if msg:
                    self.send(reduce(operator.__add__, msg), log)
        except Exception, e:
            console_logger.exception(e)
            log['exception'] = str(e)
            log['proc_t'] = int((time.time() - log['ts']) * 1000)
            logger.info(json.dumps(log))

        #self.channel.basic_ack(delivery_tag=method.delivery_tag)


    def mq_listen(self, mq_queue_name, product_key):
        name = 'rules_engine_core_{}'.format(mq_queue_name)
        while True:
            try:
                self.mq_subcribe(mq_queue_name, product_key)
                self.channel.basic_qos(prefetch_count=1)
                self.channel.basic_consume(self.consume, queue=name, no_ack=True)
                self.channel.start_consuming()
            except AMQPConnectionError, e:
                console_logger.exception(e)
                self.mq_reconnect()

    def mq_publish(self, product_key, msg_list):
        for msg in msg_list:
            if self.debug is True:
                routing_key = settings.DEBUG_ROUTING_KEY[msg.get('action_type', 'log')]
                log = {
                    'module': 're_processor',
                    'action': 'pub',
                    'ts': time.time(),
                    'topic': routing_key
                }
                while True:
                    try:
                        self.channel.basic_publish(settings.EXCHANGE, routing_key, json.dumps(msg))
                    except AMQPConnectionError, e:
                        console_logger.exception(e)
                        self.mq_reconnect()
                    else:
                        break
                log['proc_t'] = int((time.time() - log['ts']) * 1000)
                logger.info(json.dumps(log))

            routing_key = settings.PUBLISH_ROUTING_KEY[msg['action_type']]
            log = {
                'module': 're_processor',
                'action': 'pub',
                'ts': time.time(),
                'topic': routing_key
            }
            while True:
                try:
                    self.channel.basic_publish(settings.EXCHANGE, routing_key, json.dumps(msg))
                except AMQPConnectionError, e:
                    console_logger.exception(e)
                    self.mq_reconnect()
                else:
                    break
            log['proc_t'] = int((time.time() - log['ts']) * 1000)
            logger.info(json.dumps(log))

    def mq_unpack(self, body, log=None):
        log['running_status'] = 'unpack'
        msg = json.loads(body)
        #print msg;
        event =  settings.TOPIC_MAP[msg['event_type']]
        if msg.has_key('data'):
            data = msg.pop('data')
            if 'attr_fault' == msg['event_type'] or 'attr_alert' == msg['event_type']:
                msg['.'.join([event, data['attr_name']])] = data['value']
                msg['attr_displayname'] = data['attr_displayname']
            else:
                msg.update({'.'.join(['data', k]): v for k, v in data.items()})

        msg['sys.timestamp'] = int(time.time())
        msg['sys.time_now'] = time.strftime('%Y-%m-%d %a %H:%M:%S')
        msg['common.product_key'] = msg['product_key']
        msg['common.did'] = msg['did']
        msg['common.mac'] = msg['mac']

        if 'online' == event:
            msg['online.status'] = 1
            msg['offline.status'] = 0
            msg['bind.status'] = 0
            msg['unbind.status'] = 0
        elif 'offline' == event:
            msg['online.status'] = 0
            msg['offline.status'] = 1
            msg['bind.status'] = 0
            msg['unbind.status'] = 0
        elif 'bind' == event:
            msg['bind.status'] = 1
            msg['unbind.status'] = 0
            msg['bind.app_id'] = msg['app_id']
            msg['bind.uid'] = msg['uid']
        elif 'unbind' == event:
            msg['bind.status'] = 0
            msg['unbind.status'] = 1
            msg['unbind.app_id'] = msg['app_id']
            msg['unbind.uid'] = msg['uid']
        else:
            msg['bind.status'] = 0
            msg['unbind.status'] = 0


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
            __rule_tree_list = [x['task_list'] for x in rule_tree if event == x['event']]
            if __rule_tree_list:
                tmp_msg = copy.copy(msg)
                tmp_msg['common.rule_id'] = rule_id
                __rule_tree = {
                    'event': msg['event_type'],
                    'rule_id': rule_id,
                    'debug': msg.get('debug', False),
                    'test_id': msg.get('test_id', ''),
                    'msg_to': settings.MSG_TO['internal'],
                    'ts': log['ts'],
                    'current': __rule_tree_list[0][0][0] if __rule_tree_list[0] else 'tri',
                    'task_list': __rule_tree_list[0],
                    'para_task': __rule_tree_list[1:],
                    'task_vars': tmp_msg,
                    'custom_vars': custom_vars
                }
                msg_list.append(__rule_tree)

        db.close()

        return msg_list


class BaseRedismqConsumer(object):
    '''
    Base class for redismq consumer
    '''

    def redis_initial(self):
        self.redis_conn = get_redis()

    def redis_listen(self, mq_queue_name, product_key):
        while True:
            log = {
                'ts': time.time(),
                'module': 're_processor_status',
                'running_status': 'beginning'
            }
            try:
                msg = self.redis_conn.brpop('rules_engine.{0}.{1}'.format(mq_queue_name, product_key), settings.LISTEN_TIMEOUT)
                if not msg:
                    #print '{} IDLE-----'.format(mq_queue_name)
                    continue
                #print msg
                lst = self.unpack(msg[1], log)
                msg = self.process_msg(lst, log)
                if msg:
                    self.send(msg, log)
            except Exception, e:
                console_logger.exception(e)
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
        log['running_status'] = 'unpack'
        return json.loads(body)


class DefaultQueueConsumer(object):

    def default_initial(self):
        pass

    def default_listen(self, mq_queue_name, product_key):
        while True:
            log = {
                'ts': time.time(),
                'module': 're_processor_status',
                'running_status': 'beginning'
            }
            try:
                msg = self.default_queue[mq_queue_name].get(timeout=settings.LISTEN_TIMEOUT)
                msg = self.process_msg(msg, log)
                if msg:
                    self.send(msg, log)
            except Empty, e:
                #print '{} IDLE-----'.format(mq_queue_name)
                continue
            except Exception, e:
                console_logger.exception(e)
                log['exception'] = str(e)
                log['proc_t'] = int((time.time() - log['ts']) * 1000)
                logger.info(json.dumps(log))

    def default_publish(self, product_key, msg_list):
        for msg in msg_list:
            try:
                self.default_queue[msg['current']].put(msg)
            except:
                pass

    def default_unpack(self, body, log=None):
        log['running_status'] = 'unpack'


class CommonTransceiver(object):
    '''
    mixins transceiver
    '''

    def send(self, body, log=None):
        log['running_status'] = 'send'
        for _type, method in settings.TRANSCEIVER['send'].items():
            msg_list = filter(lambda x: _type == x['msg_to'], body)
            if msg_list:
                getattr(self, method)(self.product_key, msg_list)

    def unpack(self, body, log=None):
        log['running_status'] = 'unpack'
        return getattr(self, self.unpack_method)(body, log)

    def begin(self):
        self.unpack_method = settings.TRANSCEIVER['unpack'][self.receiver_type]
        self.begin_method = settings.TRANSCEIVER['begin'][self.receiver_type]
        getattr(self, self.begin_method)(self.mq_queue_name, self.product_key)

class InternalTransceiver(CommonTransceiver):
    '''
    mixins transceiver
    '''

    def init_queue(self):
        self.receiver_type = settings.MSG_TO['internal']
        getattr(self, settings.TRANSCEIVER['init'][settings.MSG_TO['internal']])()


class MainTransceiver(CommonTransceiver):
    '''
    mixins transceiver
    '''

    def init_queue(self):
        self.receiver_type = settings.MSG_TO['external']
        getattr(self, settings.TRANSCEIVER['init'][settings.MSG_TO['external']])()
        getattr(self, settings.TRANSCEIVER['init'][settings.MSG_TO['internal']])()


class OutputTransceiver(CommonTransceiver):
    '''
    mixins transceiver
    '''

    def init_queue(self):
        self.receiver_type = settings.MSG_TO['internal']
        getattr(self, settings.TRANSCEIVER['init'][settings.MSG_TO['external']])()
        getattr(self, settings.TRANSCEIVER['init'][settings.MSG_TO['internal']])()
