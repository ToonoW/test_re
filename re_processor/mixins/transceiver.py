#!/usr/bin/env python
# coding=utf-8

import time, json

from re_processor import settings
from re_processor.connections import BaseRabbitmqConsumer, BaseRedismqConsumer, get_mysql
from re_processor.common import logger


class RabbitmqTransmitter(BaseRabbitmqConsumer):
    '''
    mixins transmitter using rabbitmq
    '''
    transmitter_init = 'mq_initial'

    def pack(self, body, log=None):
        pass


class RabbitmqReceiver(BaseRabbitmqConsumer):
    '''
    mixins receiver using rabbitmq
    '''
    receiver_init = 'mq_initial'

    def unpack(self, body, log=None):
        msg = json.loads(body)
        task_msg = {}
        if msg.has_key('data'):
            data = msg.pop('data')
            if 'attr_fault' == msg['event_type'] or 'attr_alert' == msg['event_type']:
                msg[data['attr_name']] = data['value']
                msg['attr_displayname'] = data['attr_displayname']
            else:
                msg.update(data)

        db = get_mysql()
        sql = 'select `id`, `rule_tree`, `custom_vars` from {0} where `obj_id`={1} or where `obj_id`={2}'.format(
            settings.MYSQL_TABLE['rule']['table'],
            msg['did'],
            msg['product_key'])
        db.execute(sql)
        for rule_id, rule_tree, custom_vars in db.fetchall():
            event =  settings.TOPIC_MAP[msg['event_type']]
            rule_tree = json.loads(rule_tree) if rule_tree else rule_tree
            custom_vars = json.loads(custom_vars) if custom_vars else custom_vars
            rule_tree_list = [{'event': msg['event_type'],
                        'ts': log['ts'],
                        'task': x['task_list'][0],
                        'task_list': x['task_list'][1:],
                        'task_vars': msg, 'custom_vars': custom_vars,
                        'result': []}
                        for x in rule_tree if event == x['event'] and x['task_list']]

    def begin(self, queue, product_key):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.consume, queue=queue)
        self.channel.start_consuming()


class RedisTransmitter(BaseRedismqConsumer):
    '''
    mixins transmitter using rabbitmq
    '''
    transmitter_init = 'redis_initial'

    def pack(self, body, log=None):
        pass


class RedisReceiver(BaseRedismqConsumer):
    '''
    mixins transmitter using rabbitmq
    '''
    receiver_init = 'redis_initial'

    def unpack(self, body, log=None):
        pass

    def begin(self, queue, product_key):
        self.redis_listen(queue, product_key)
