#!/usr/bin/env python
# coding=utf-8


import time, json

from re_processor.mixins.tranceiver import BaseRabbitmqConsumer
from re_processor import settings
from re_processor.common import debug_logger as logger, console_logger
from re_processor.common import _log

class MainDispatcher(BaseRabbitmqConsumer):

    def __init__(self):
        self.mq_initial()
        self.sender = MainSender()

    def consume(self, ch, method, properties, body):
        log = {
            'ts': time.time(),
            'module': 're_processor_status',
            'running_status': 'beginning'
        }
        try:
            #print body
            lst = self.unpack(body, log)
        except Exception, e:
            console_logger.exception(e)
            log['exception'] = str(e)
            log['proc_t'] = int((time.time() - log['ts']) * 1000)
            logger.info(json.dumps(log))


class MainSender(BaseRabbitmqConsumer):

    def __init__(self):
        self.mq_initial()

    def send(self, msg):
        pass
