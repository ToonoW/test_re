#!/usr/bin/env python
# coding=utf-8

import time, json

from re_processor import settings
from re_processor.connections import BaseRabbitmqConsumer
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
        pass

    def begin(self, queue):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.consume, queue=queue)
        self.channel.start_consuming()

