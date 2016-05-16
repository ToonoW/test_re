#!/usr/bin/env python
# coding=utf-8

import sys, os
import dotenv
dotenv.read_dotenv(os.getcwd()+'/.env')

sys.path.append(os.environ.get('SYS_PATH', '.'))

import json
from docopt import docopt

from re_processor.connections import get_mongodb, get_mysql
from re_processor.consumer import BaseRabbitmqConsumer
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
    product_key = '8345956355714fe19e074a241837accd'
    routing_key = settings.ROUTING_KEY['data'].format(product_key)
    did = 'xB6nh4FR5f25MaqdA7rTuU'

    #red = get_redis()
    #print red.rpop('rules_engine.log.*')

    #test_consumer = ConsumeEvent('gw_notification_message')
    #test_consumer = ConsumeEvent('rules_engine_debug')
    #test_consumer = ConsumeEvent('rules_engine_http')
    #test_consumer.start()
    #test_consumer.fetch_publish_msg('rules_engine_debug')

    #db = get_mongodb()
    #ds = db['device_status']

    #status = ds.find_one({'did': did})
    #result = status['attr']['0']
    #print '\n'
    #for key, val in status.items():
    #    print key, val
    #    print '\n'
    #print result

    db = get_mongodb('core')
    ds = db['datapoints']

    status = ds.find_one({'product_key': product_key})
    print '\n'

    for val in status['datas']['entities'][0]['attrs']:
        print val
        print '\n'
    #print status

    #db = get_mysql()
    #sql = 'select `id`, `rule_tree`, `custom_vars` from `{0}` where `obj_id`="{1}" or `obj_id`="{2}"'.format(
    #    settings.MYSQL_TABLE['rule']['table'],
    #    did,
    #    product_key)
    #db.execute(sql)
    #print db.fetchall()

