#!/usr/bin/env python
# coding=utf-8

import time, json, copy
from collections import defaultdict

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
        self.task_dict = defaultdict(list)
        self.sorted_task_queue_dict = defaultdict(list)
        self.task_queue_head_dict = defaultdict(int)
        #self.processor = MainProcessor(MainSender(self.product_key))

    def consume(self, ch, method, properties, body):
        log = {
            'ts': time.time(),
            'module': 're_processor_status',
            'running_status': 'beginning'
        }
        try:
            print body
        except Exception, e:
            console_logger.exception(e)
            log['exception'] = str(e)
            log['proc_t'] = int((time.time() - log['ts']) * 1000)
            logger.info(json.dumps(log))

    def begin(self):
        self.mq_listen(self.mq_queue_name, self.product_key)

    def waiting(self, body, log):
        '''
        msg:
        {
            "action_type": "schedule_wait",
            "ver": 3,
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

        if msg['ts'] not in self.task_dict:
            for i in xrange(0, settings.TASK_QUEUE_NUM):
                end = len(self.sorted_task_queue_dict[i])
                if end < settings.TASK_QUEUE_LEN:
                    self.insertion_sort(self.sorted_task_queue_dict[i], msg['ts'], self.task_queue_head_dict[i], end-1)
                    if i > 0.8 * settings.TASK_QUEUE_NUM:
                        log['warning'] = 'scheduled queue is nearly full: {0} of {1}'.format(i, settings.TASK_QUEUE_NUM)
                        logger.warning(json.dumps(log))
                    break
            else:
                self.insertion_sort(self.sorted_task_queue_dict[1001], msg['ts'], self.task_queue_head_dict[1001], end-1)
                log['error'] = 'scheduled queue is full: {0} of {1}'.format(settings.TASK_QUEUE_NUM, settings.TASK_QUEUE_NUM)
                logger.error(json.dumps(log))

        self.task_dict[msg['ts']].append({
            'did': msg['did'],
            'rule_id': msg['rule_id'],
            'node_id': msg['node_id']
        })

    def insertion_sort(sorted_list, x, start, end):
        if not sorted_list or x >= sorted_list[-1]:
            sorted_list.append(x)
        else:
            def _binary_search(l, r):
                if l == r:
                    return l - 1 if x < sorted_list[l] else l + 1
                mid = (l+r)/2
                if x < sorted_list[mid]:
                    return _binary_search(l, mid-1)
                else:
                    return _binary_search(mid+1, r)

            pos = _binary_search(start, end)
            sorted_list.insert(pos, x)

    def dispatch():
        while True:
            try:
                log = {
                    'ts': time.time(),
                    'module': 're_processor_status',
                    'running_status': 'dispatching'
                }
            except:
                pass
