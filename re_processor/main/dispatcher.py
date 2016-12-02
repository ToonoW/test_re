#!/usr/bin/env python
# coding=utf-8

import time, json, copy, os

import gevent

from re_processor.mixins.transceiver import BaseRabbitmqConsumer
from re_processor import settings
from re_processor.common import debug_logger as logger, console_logger

from processor import MainProcessor

class MainDispatcher(BaseRabbitmqConsumer):
    '''
    handle msgs like (device_online, device_offline, device_bind, device_unbind, device_status_kv, attr_fault, attr_alert)
    '''

    def __init__(self, mq_queue_name, product_key=None):
        self.mq_queue_name = mq_queue_name
        self.product_key = product_key or '*'
        self.mq_initial()
        self.processor = MainProcessor(MainSender(self.product_key))

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

        #self.channel.basic_ack(delivery_tag=method.delivery_tag)

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

    def send(self, msg):
        self.mq_publish(self.product_key, [msg])

    def send_msgs(self, msgs):
        self.mq_publish(self.product_key, msgs)


class ScheduleDispatcher(BaseRabbitmqConsumer):
    '''
    handle msgs waiting to be dispatched
    '''

    def __init__(self, product_key=None):
        self.mq_queue_name = 'schedule_wait'
        self.product_key = product_key or '*'
        self.mq_initial()

    def consume(self, ch, method, properties, body):
        log = {
            'ts': time.time(),
            'module': 're_processor_status',
            'running_status': 'beginning'
        }
        try:
            #print body
            gevent.spawn(self.waiting, body, log)
        except Exception, e:
            console_logger.exception(e)
            log['exception'] = str(e)
            log['proc_t'] = int((time.time() - log['ts']) * 1000)
            logger.info(json.dumps(log))

    def begin(self):
        self.id = ""
        self.mq_listen(self.mq_queue_name, self.product_key)

    def waiting(self, body, log):
        '''
        msg:
        {
            "action_type": "schedule_wait",
            "product_key": <product_key string>,
            "did": <did string>,
            "mac": <mac string>,
            "rule_id": <rule_id int>,
            "node_id" <node_id string>,
            "ts": <timestamp_in_seconds int>
        }
        '''
        log['running_status'] = 'waiting'
        msg = json.loads(body)
        print msg
        self.persist(msg)


    def dispatch(self):
        while True:
            try:
                time_now = time.time()
                log = {
                    'ts': time_now,
                    'module': 're_processor_status',
                    'running_status': 'dispatching'
                }
            except:
                pass

    def persist(self, msg):
        hour = msg['ts'] / 3600
        minute = msg['ts'] / 60
        file_dir = settings.SCHEDULE_FILE_DIR.rstrip('/') + '/{0}/{1}/{2}/{3}/{4}'.format(
            self.id,
            hour,
            minute,
            msg['ts'],
            hash(msg.get('did') or msg['product_key']) % settings.DEVICE_HASH_GROUP
            )
        if not os.path.exists(file_dir):
            os.makedirs(file_dir, mode=777)

        file_name = '{0}/{1}_{2}_{3}_{4}'.format(
            file_dir,
            msg.get('did', ''),
            msg['rule_id'],
            msg['node_id'],
            msg['ts']
            )
        os.system(r'touch {}'.format(file_name))
