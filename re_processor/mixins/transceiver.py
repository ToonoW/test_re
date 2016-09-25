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
        self.connecting = False
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
                self.connecting = True
                time.sleep(5)
                self.m2m_conn = BlockingConnection(URLParameters(settings.M2M_MQ_URL))
            except AMQPConnectionError, e:
                console_logger.exception(e)
                continue
            else:
                self.channel = self.m2m_conn.channel()
                self.connecting = False
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
            msg_pub = json.dumps(msg)
            if self.debug is True:
                routing_key = settings.DEBUG_ROUTING_KEY[msg.get('action_type', 'log')]
                log = {
                    'module': 're_processor',
                    'action': 'pub',
                    'ts': time.time(),
                    'topic': routing_key,
                    'msg': msg_pub
                }
                while True:
                    try:
                        if self.connecting:
                            time.sleep(5)
                            continue
                        self.channel.basic_publish(settings.EXCHANGE, routing_key, msg_pub)
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
                'topic': routing_key,
                'msg': msg_pub
            }
            while True:
                try:
                    if self.connecting:
                        time.sleep(5)
                        continue
                    self.channel.basic_publish(settings.EXCHANGE, routing_key, msg_pub)
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

        msg['sys.timestamp_ms'] = int(log['ts'] * 1000)
        msg['sys.timestamp'] = int(log['ts'])
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
        sql = 'select `id`, `rule_tree`, `custom_vars`, `enabled`, `ver` from `{0}` where `obj_id`="{1}" or `obj_id`="{2}"'.format(
            settings.MYSQL_TABLE['rule']['table'],
            msg['did'],
            msg['product_key'])
        db.execute(sql)
        msg_list = []
        for rule_id, rule_tree, custom_vars, enabled, ver in db.fetchall():
            if 1 != enabled:
                continue
            rule_tree = json.loads(rule_tree) if rule_tree else []
            custom_vars = json.loads(custom_vars) if custom_vars else {}

            tmp_msg = copy.copy(msg)
            tmp_msg['common.rule_id'] = rule_id
            if 3 == ver:
                for __task in rule_tree['event'][event]:
                    __rule_tree = {
                        'ver': ver,
                        'event': msg['event_type'],
                        'rule_id': rule_id,
                        'action_id_list': [],
                        'msg_to': settings.MSG_TO['internal'],
                        'ts': log['ts'],
                        'current': __task,
                        'task_list': rule_tree['task_list'],
                        'task_vars': tmp_msg,
                        'custom_vars': custom_vars
                    }
                    msg_list.append(__rule_tree)
            elif 2 == ver:
                __rule_tree_list = [x['task_list'] for x in rule_tree if event == x['event']]
                if __rule_tree_list:
                    _task = __rule_tree_list[0].pop(0)
                    triggled = []
                    while __rule_tree_list[0]:
                        if _task['task_list']:
                            break
                        else:
                            triggled += _task['can_tri']
                            _task = __rule_tree_list[0].pop(0)

                    __rule_tree = {
                        'ver': ver,
                        'event': msg['event_type'],
                        'rule_id': rule_id,
                        'action_id_list': [],
                        'msg_to': settings.MSG_TO['internal'],
                        'ts': log['ts'],
                        'action_sel': True,
                        'can_tri': _task['action'],
                        'triggle': triggled if _task['task_list'] else triggled + _task['action'],
                        'current': 'tri' if not __rule_tree_list[0] and not _task['task_list'] else _task['task_list'][0][0],
                        'task_list': _task['task_list'],
                        'para_task': [],
                        'todo_task': __rule_tree_list[0],
                        'task_vars': tmp_msg,
                        'custom_vars': custom_vars
                    }
                    msg_list.append(__rule_tree)
            elif 1 == ver:
                __rule_tree_list = [x['task_list'] for x in rule_tree if event == x['event']]
                if __rule_tree_list:
                    __rule_tree = {
                        'ver': ver,
                        'event': msg['event_type'],
                        'rule_id': rule_id,
                        'action_id_list': [],
                        'msg_to': settings.MSG_TO['internal'],
                        'ts': log['ts'],
                        'action_sel': False,
                        'can_tri': [],
                        'triggle': [],
                        'current': __rule_tree_list[0][0][0] if __rule_tree_list[0] else 'tri',
                        'task_list': __rule_tree_list[0],
                        'para_task': __rule_tree_list[1:],
                        'todo_task': [],
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
