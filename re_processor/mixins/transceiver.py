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
from re_processor.common import (
    debug_logger as logger,
    console_logger,
    new_virtual_device_log,
    update_several_sequence,
    update_device_status,
    cache_rules,
    get_rules_from_cache,
    getset_last_data,
    check_interval_locked,
    _log)


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
                time.sleep(1)
                self.m2m_conn = BlockingConnection(URLParameters(settings.M2M_MQ_URL))
            except AMQPConnectionError, e:
                console_logger.exception(e)
                continue
            else:
                self.channel = self.m2m_conn.channel()
                self.connecting = False
                break

    def consume(self, ch, method, properties, body):
        '''
        subclass implement
        '''
        print body


    def mq_listen(self, mq_queue_name, product_key, no_ack=True):
        name = 'rules_engine_core_{}'.format(mq_queue_name)
        while True:
            try:
                self.mq_subcribe(mq_queue_name, product_key)
                self.channel.basic_qos(prefetch_count=1)
                self.channel.basic_consume(self.consume, queue=name, no_ack=no_ack)
                self.channel.start_consuming()
            except AMQPConnectionError, e:
                console_logger.exception(e)
                self.mq_reconnect()

    def mq_publish(self, product_key, msg_list):
        for msg in msg_list:
            msg_pub = json.dumps(msg)
            if settings.DEBUG is True:
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
                            time.sleep(1)
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
                        time.sleep(1)
                        continue
                    self.channel.basic_publish(settings.EXCHANGE, routing_key, msg_pub)
                except AMQPConnectionError, e:
                    console_logger.exception(e)
                    self.mq_reconnect()
                else:
                    break
            log['proc_t'] = int((time.time() - log['ts']) * 1000)
            logger.info(json.dumps(log))

    def mq_unpack(self, msg, log=None):
        log['running_status'] = 'unpack'
        #print msg;

        if 'device_status_kv' == msg['event_type']:
            return self.generate_msg_list_data(msg, log)
        elif msg['event_type'] in ['device_online', 'device_offline']:
            return self.generate_msg_list_on_offline(msg, log)
        elif 'device_schedule' == msg['event_type']:
            return self.generate_msg_list_schedule(msg, log)
        else:
            return self.generate_msg_list(msg, log)

    def generate_msg_list_on_offline(self, msg, log):
        event =  settings.TOPIC_MAP[msg['event_type']]

        msg['sys.timestamp_ms'] = int(log['ts'] * 1000)
        msg['sys.timestamp'] = int(log['ts'])
        msg['sys.time_now'] = time.strftime('%Y-%m-%d %a %H:%M:%S')
        msg['common.did'] = msg['did']
        msg['common.mac'] = msg['mac'].lower()
        msg['common.mac_upper'] = msg['mac'].upper()
        msg['common.product_key'] = msg['product_key']

        if 'online' == event:
            msg['online.status'], msg['offline.status'] = 1, 0
        else:
            msg['online.status'], msg['offline.status'] = 0, 1

        db = get_mysql()
        sql = 'select `id`, `rule_tree`, `custom_vars`, `enabled`, `ver`, `type`, `interval`, `obj_id`, `params` from `{0}` where `obj_id`="{1}" or `obj_id`="{2}"'.format(
            settings.MYSQL_TABLE['rule']['table'],
            msg['did'],
            msg['product_key'])
        db.execute(sql)

        msg_list = []
        cache_rule = defaultdict(list)
        for rule_id, rule_tree, custom_vars, enabled, ver, type, interval, obj_id, params in db.fetchall():
            if 1 != enabled or check_interval_locked(rule_id, msg['did']):
                continue
            rule_tree = json.loads(rule_tree) if rule_tree else []
            custom_vars = json.loads(custom_vars) if custom_vars else {}

            tmp_msg = copy.copy(msg)
            tmp_msg['common.rule_id'] = rule_id
            log_id = ''
            cache_rule[obj_id].append({
                'ver': ver,
                'rule_id': rule_id,
                'rule_tree': rule_tree,
                'custom_vars': custom_vars,
                'params': json.loads(params) if params else [],
                'type': type,
                'interval': interval
            })
            if 3 == ver:
                if rule_tree.get('schedule_list', []):
                    if 'online' == event:
                        update_device_status(msg['product_key'], msg['did'], msg['mac'], 1, msg['sys.timestamp_ms'])
                        msg_list.extend([{
                            'action_type': 'schedule_wait',
                            'product_key': msg['product_key'],
                            'did': msg['did'],
                            'mac': msg['mac'],
                            'rule_id': rule_id,
                            'node_id': x['node'],
                            'msg_to': settings.MSG_TO['external'],
                            'ts': msg['sys.timestamp'] + 60*x['interval'],
                            'flag': str(msg['sys.timestamp_ms']),
                            'once': False
                        } for x in rule_tree['schedule_list']])
                    else:
                        update_device_status(msg['product_key'], msg['did'], msg['mac'], 0, msg['sys.timestamp_ms'])

                if rule_tree['event'].get(event, []):
                    if 'virtual:site' == msg['mac']:
                        log_id = new_virtual_device_log(msg['product_key'], rule_id)
                    msg_list.extend(self.v3_msg(event, rule_tree, msg, custom_vars, rule_id, interval, type, log_id, log))

            elif 1 == ver:
                msg_list.extend(self.v1_msg(event, rule_tree, msg, custom_vars, rule_id, interval, type, log_id, log))

        db.close()

        cache_rules(cache_rule, msg['product_key'])

        return msg_list

    def generate_msg_list_schedule(self, msg, log):
        db = get_mysql()
        sql = 'select `id`, `product_key`, `rule_tree`, `custom_vars`, `enabled`, `ver` from `{0}` where `id`={1}'.format(
            settings.MYSQL_TABLE['rule']['table'],
            msg['rule_id'])
        db.execute(sql)
        result = db.fetchall()
        if not result:
            return []

        rule_id, product_key, rule_tree, custom_vars, enabled, ver = result[0]

        if 1 != enabled:
            return []

        rule_tree = json.loads(rule_tree) if rule_tree else []
        custom_vars = json.loads(custom_vars) if custom_vars else {}

        node = rule_tree['task_list'].get(msg['node_id'], {})
        msg['sys.timestamp_ms'] = int(log['ts'] * 1000)
        msg['sys.timestamp'] = int(log['ts'])
        msg['sys.time_now'] = time.strftime('%Y-%m-%d %a %H:%M:%S')
        msg['common.did'] = msg['did']
        msg['common.mac'] = msg['mac'].lower()
        msg['common.mac_upper'] = msg['mac'].upper()
        msg['product_key'] = product_key
        msg['common.product_key'] = msg['product_key']
        msg_list = []
        log_id = ''

        if msg['did'] and msg['once'] is False:
            if not node or \
               'schedule' != node['content']['event'] or \
               bool(msg['did']) != node['content']['is_device']:
                db.close()
                return []

            sql = 'select `ts`, `is_online` from `{0}` where `did`="{1}" and `mac`="{2}" limit 1'.format(
                settings.MYSQL_TABLE['device_status']['table'],
                msg['did'],
                msg['mac'])

            db.execute(sql)
            res = db.fetchall()
            if not res:
                db.close()
                return []

            flag, is_online = res[0]
            if flag != msg['flag']:
                db.close()
                return []

            log_id = new_virtual_device_log(product_key, rule_id) if 'virtual:site' == msg['mac'] else ''

            msg_list.append({
                'action_type': 'schedule_wait',
                'product_key': product_key,
                'did': msg['did'],
                'mac': msg['mac'],
                'rule_id': rule_id,
                'node_id': node['id'],
                'msg_to': settings.MSG_TO['external'],
                'ts': msg['sys.timestamp'] + 60*node['content']['interval'],
                'flag': flag,
                'once': False
            })

        msg_list.append({
            'ver': ver,
            'event': msg['event_type'],
            'rule_id': rule_id,
            'log_id': log_id,
            'msg_to': settings.MSG_TO['internal'],
            'ts': log['ts'],
            'current': node,
            'task_list': rule_tree['task_list'],
            'task_vars': copy.copy(msg),
            'extern_params': {},
            'custom_vars': custom_vars
        })
        db.close()

        return msg_list

    def generate_msg_list_data(self, msg, log):
        event = 'data'
        rules_list = get_rules_from_cache(msg['product_key'], msg['did'])
        if not rules_list:
            return []

        data = msg.pop('data')
        msg.update({'.'.join(['data', k]): v for k, v in data.items()})
        last_data = None

        msg['sys.timestamp_ms'] = int(log['ts'] * 1000)
        msg['sys.timestamp'] = int(log['ts'])
        msg['sys.time_now'] = time.strftime('%Y-%m-%d %a %H:%M:%S')
        msg['common.did'] = msg['did']
        msg['common.mac'] = msg['mac'].lower()
        msg['common.mac_upper'] = msg['mac'].upper()
        msg['common.product_key'] = msg['product_key']

        msg_list = []
        sequence_dict = {}
        for rule in rules_list:
            if check_interval_locked(rule['rule_id'], msg['did']):
                continue
            tmp_msg = copy.copy(msg)
            tmp_msg['common.rule_id'] = rule['rule_id']
            log_id = ''
            if 3 == rule['ver']:
                if rule['rule_tree'].get('sequence_list', []):
                    sequence_dict.update({'re_core_{0}_{1}_device_sequence'.format(msg['did'], __task['content']['data']): tmp_msg.get(__task['content']['data'], '') for __task in rule['rule_tree']['sequence_list']})

                if 2 == rule['type']:
                    if last_data is None:
                        last_data = getset_last_data(data, msg['did'])
                    if reduce(lambda res, y: res and data.get(y, None) is not None and last_data.get(y, None) == data.get(y, None), rule['params'], True):
                        continue

                if rule['rule_tree']['event'].get(event, []):
                    if 'virtual:site' == msg['mac'] and not log_id:
                        log_id = new_virtual_device_log(msg['product_key'], rule['rule_id'])

                    msg_list.extend(self.v3_msg(event, rule['rule_tree'], msg, rule['custom_vars'], rule['rule_id'], rule['interval'], rule['type'], log_id, log))

            elif 1 == rule['ver']:
                if 2 == rule['type']:
                    if last_data is None:
                        last_data = getset_last_data(data, msg['did'])
                    if reduce(lambda res, y: res and data.get(y, None) is not None and last_data.get(y, None) == data.get(y, None), rule['params'], True):
                        continue

                msg_list.extend(self.v1_msg(event, rule['rule_tree'], msg, rule['custom_vars'], rule['rule_id'], rule['interval'], rule['type'], log_id, log))

        if sequence_dict:
            result = update_several_sequence(sequence_dict)
            if result is not True:
                _log(dict(log, **result))

        return msg_list

    def generate_msg_list(self, msg, log):
        event =  settings.TOPIC_MAP[msg['event_type']]
        rules_list = get_rules_from_cache(msg['product_key'], msg['did'])
        if not rules_list:
            return []

        if 'attr_fault' == msg['event_type'] or 'attr_alert' == msg['event_type']:
            data = msg.pop('data')
            msg['.'.join([event, data['attr_name']])] = data['value']

        elif 'bind' == event:
            msg['bind.status'], msg['unbind.status'] = 1, 0
            msg['bind.app_id'] = msg['app_id']
            msg['bind.uid'] = msg['uid']

        elif 'unbind' == event:
            msg['bind.status'], msg['unbind.status'] = 0, 1
            msg['unbind.app_id'] = msg['app_id']
            msg['unbind.uid'] = msg['uid']

        else:
            return []

        msg['sys.timestamp_ms'] = int(log['ts'] * 1000)
        msg['sys.timestamp'] = int(log['ts'])
        msg['sys.time_now'] = time.strftime('%Y-%m-%d %a %H:%M:%S')
        msg['common.did'] = msg['did']
        msg['common.mac'] = msg['mac'].lower()
        msg['common.mac_upper'] = msg['mac'].upper()
        msg['common.product_key'] = msg['product_key']

        msg_list = []
        for rule in rules_list:
            if check_interval_locked(rule['rule_id'], msg['did']):
                continue
            tmp_msg = copy.copy(msg)
            tmp_msg['common.rule_id'] = rule['rule_id']
            log_id = ''
            if 3 == rule['ver']:
                if rule['rule_tree']['event'].get(event, []):
                    if 'virtual:site' == msg['mac'] and not log_id:
                        log_id = new_virtual_device_log(msg['product_key'], rule['rule_id'])

                    msg_list.extend(self.v3_msg(event, rule['rule_tree'], msg, rule['custom_vars'], rule['rule_id'], rule['interval'], rule['type'], log_id, log))

            elif 1 == rule['ver']:
                msg_list.extend(self.v1_msg(event, rule['rule_tree'], msg, rule['custom_vars'], rule['rule_id'], rule['interval'], rule['type'], log_id, log))

        return msg_list

    def v3_msg(self, event, rule_tree, msg, custom_vars, rule_id, interval, rule_type, log_id, log):
        return [{
            'ver': 3,
            'event': msg['event_type'],
            'rule_id': rule_id,
            'log_id': log_id,
            'msg_to': settings.MSG_TO['internal'],
            'ts': log['ts'],
            'current': __task,
            'type': rule_type,
            'interval': interval,
            'task_list': rule_tree['task_list'],
            'task_vars': dict(msg, **{'common.rule_id': rule_id}),
            'extern_params': {},
            'custom_vars': custom_vars
        } for __task in rule_tree['event'].get(event, [])]

    def v1_msg(self, event, rule_tree, msg, custom_vars, rule_id, interval, rule_type, log_id, log):
        __rule_tree_list = [x['task_list'] for x in rule_tree if event == x['event']]
        return [{
            'ver': 1,
            'event': msg['event_type'],
            'rule_id': rule_id,
            'log_id': log_id,
            'action_id_list': [],
            'msg_to': settings.MSG_TO['internal'],
            'ts': log['ts'],
            'current': __rule_tree_list[0][0][0] if __rule_tree_list[0] else 'tri',
            'type': rule_type,
            'interval': interval,
            'task_list': __rule_tree_list[0],
            'para_task': __rule_tree_list[1:],
            'task_vars': dict(msg, **{'common.rule_id': rule_id}),
            'custom_vars': custom_vars
        }] if __rule_tree_list else []


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
