#!/usr/bin/env python
# coding=utf-8

import json
from docopt import docopt

from re_processor.mixins.consumer import BaseRabbitmqConsumer

from re_processor.connections import get_mongodb
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
        if not body:
            return {}
        return json.loads(body)


if '__main__' == __name__:
    #args = docopt(__doc__, version='Gizwits Data Recorder 0.0')
    #print args
    product_key = '8345956355714fe19e074a241837accd'
    routing_key = settings.ROUTING_KEY['data'].format(product_key)
    did = 'xB6nh4FR5f25MaqdA7rTuU'

    db = get_mongodb()
    ds = db['device_status']

    status = ds.find_one({'did': did})
    result = status['attr']['0']
    print '\n'
    for key, val in status.items():
        print key, val
        print '\n'
    print result