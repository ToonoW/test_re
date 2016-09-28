#!/usr/bin/env python
# coding=utf-8

import sys, os
import dotenv
dotenv.read_dotenv(os.getcwd()+'/.env')

sys.path.append(os.environ.get('SYS_PATH', '.'))

import json
from docopt import docopt

from re_processor.connections import get_mongodb, get_mysql
from re_processor.consumer.BaseConsumer import BaseRabbitmqConsumer
from re_processor import settings


class ConsumeEvent(BaseRabbitmqConsumer):

    def callback(self, ch, method, properties, body):
        print ch

    def subscribe_msg(self, queue, routing_key, exchange='amq.topic'):
        self.channel.queue_declare(
            queue=queue,
            auto_delete=True)
        self.channel.queue_bind(
            exchange=exchange,
            queue=queue,
            routing_key=routing_key)

    def cancel_queue(self, queue, routing_key, exchange='amq.topic'):
        self.channel.queue_unbind(
            queue=queue,
            exchange=exchange,
            routing_key=routing_key)

        # 消费清空队列
        self.channel.basic_consume(self.callback, queue=queue)

    def fetch_publish_msg(self, queue):
        self.channel.confirm_delivery()
        method_frame, header_frame, body = self.channel.basic_get(queue=queue)
        #print method_frame, header_frame, body
        if not body:
            return {}
        #self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        return json.loads(body)


if '__main__' == __name__:
    #args = docopt(__doc__, version='Gizwits Data Recorder 0.0')
    #print args
    product_key = '8ab607683fa14e58aa17c6ed95b34556'
    routing_key = settings.ROUTING_KEY['data'].format(product_key)
    did = 'WMJpRTHfXDqqtU94ymidjL'

    #red = get_redis()
    #print red.rpop('rules_engine.log.*')

    #test_consumer = ConsumeEvent('gw_notification_message')
    #test_consumer = ConsumeEvent('rules_engine_debug')
    #test_consumer = ConsumeEvent('rules_engine_http')
    #test_consumer.start()
    #test_consumer.fetch_publish_msg('rules_engine_debug')

    db = get_mongodb()
    ds = db['device_status']

    status = ds.find_one({'did': did})
    result = status['attr']['0']
    print '\n'
    for key, val in status.items():
        print key, val
        print '\n'
    print result

    #db = get_mongodb('core')
    #ds = db['datapoints']

    #status = ds.find_one({'product_key': product_key})
    #print '\n'

    #for val in status['datas']['entities'][0]['attrs']:
    #    print val
    #    print '\n'
    #status.pop('_id')
    #print json.dumps(status)

    #db = get_mongodb('core')
    #print db.collection_names()
    #ds = db['parsers']

    #jsn = [
    #    {
    #        'name': 'leakage',
    #        'display_name': u'漏水报警',
    #        'type': 'alert',
    #        'data_type': 'bool'
    #    },
    #    {
    #        'name': 'life1',
    #        'display_name': u'滤芯1寿命',
    #        'val': 1,
    #        'type': 'alert',
    #        'data_type': 'bool'
    #    },
    #    {
    #        'name': 'life2',
    #        'display_name': u'滤芯2寿命',
    #        'val': 1,
    #        'type': 'alert',
    #        'data_type': 'bool'
    #    },
    #    {
    #        'name': 'life3',
    #        'display_name': u'滤芯3寿命',
    #        'val': 1,
    #        'type': 'alert',
    #        'data_type': 'bool'
    #    }
    #]
    #ds.update({'product_key': product_key}, {"$set": {"ext_data_points": jsn}})

    #status = ds.find_one({'product_key': product_key})
    #print '\n'
    #dp = []
    #status.pop('_id')
    #print json.dumps(status)
    #print status
    #if status and status['ext_data_points']:
    #    for attr in status['ext_data_points']:
    #        dp.append({
    #            'display_name': attr['display_name'],
    #            'name': attr['name'],
    #            'detail': attr['type'],
    #            'type': attr['data_type'],
    #            'enum': attr['range'].split(',') if 'enum' == attr['data_type'] else []
    #        })

    #print json.dumps(dp)

    #db = get_mysql()
    #sql = 'select `id`, `rule_tree`, `custom_vars` from `{0}` where `obj_id`="{1}" or `obj_id`="{2}"'.format(
    #    settings.MYSQL_TABLE['rule']['table'],
    #    did,
    #    product_key)
    #db.execute(sql)
    #print db.fetchall()

