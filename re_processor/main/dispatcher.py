#!/usr/bin/env python
# coding=utf-8


import time, json, copy

import gevent

from re_processor.mixins.transceiver import BaseRabbitmqConsumer
from re_processor import settings
from re_processor.common import debug_logger as logger, console_logger

from processor import MainProcessor

class MainDispatcher(BaseRabbitmqConsumer):

    def __init__(self, mq_queue_name, product_key=None):
        self.mq_queue_name = mq_queue_name
        self.product_key = product_key or '*'
        self.mq_initial()
        self.processor = MainProcessor(MainSender(self.product_key))
        self.debug = settings.DEBUG

    def consume(self, ch, method, properties, body):
        log = {
            'ts': time.time(),
            'module': 're_processor_status',
            'running_status': 'beginning'
        }
        try:
            #print body
            gevent.spawn(self.dispatch, body, log)
        except Exception, e:
            console_logger.exception(e)
            log['exception'] = str(e)
            log['proc_t'] = int((time.time() - log['ts']) * 1000)
            logger.info(json.dumps(log))

    def dispatch(self, body, log):
        lst = self.mq_unpack(body, log)
        if lst:
            map(lambda x: gevent.spawn(self.processor.process_msg, x, copy.deepcopy(log)), lst)

    def begin(self):
        self.mq_listen(self.mq_queue_name, self.product_key)


class MainSender(BaseRabbitmqConsumer):

    def __init__(self, product_key=None):
        self.product_key = product_key or '*'
        self.mq_initial()
        self.debug = settings.DEBUG

    def send(self, msg):
        self.mq_publish(self.product_key, [msg])
