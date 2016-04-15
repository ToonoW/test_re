#!/usr/bin/env python
# coding=utf-8

import time

from re_processor.mixins import core as core_mixins
from re_processor import settings
from re_processor.common import debug_logger as logger


class CommonProcessor(object):
    '''
    common processor
    '''

    def init_processor(self, pos='internal'):
        core_map = settings.CORE_MAP.get(pos, None)
        self.core = {k: getattr(core_mixins, v)() for k, v in core_map.items()} if core_map else None

    def process_msg(self, msg, log=None):
        '''
        return a list of msg
        '''

        log['status'] = 'process'
        ts = time.time()
        if not getattr(self, 'core', None):
            return [msg]
        msg_list = []
        p_log = []
        task_type = 'type_error'
        error_message = ''
        try:
            task_type = msg['current']
            result, msg_list, log_flag = self.core[task_type].process(msg)
        except Exception, e:
            log_flag = True
            result = 'exception'
            #error_message = e.message
            error_message = str(e)
            logger.exception(e)

        if log_flag:
            p_log = {
                'msg_to': settings.MSG_TO['internal'],
                'module': 're_processor',
                'rule_id': msg.get('rule_id', ''),
                'event': msg.get('event', ''),
                'product_key': msg['task_vars'].get('product_key', ''),
                'did': msg['task_vars'].get('did', ''),
                'mac': msg['task_vars'].get('mac', ''),
                'task_vars': msg['task_vars'],
                'current': 'log',
                'result': result,
                'ts': ts,
                'proc_t': (time.time() - ts) * 1000,
                'error_message': error_message,
                'task_type': task_type,
                'action': 'action' if 'tri' == task_type else 'rule'
            }
            if msg.get('debug') is True and self.debug is True:
                p_log['debug'] = True
                p_log['test_id'] = msg.get('test_id', '')
            msg_list.append(p_log)

        return msg_list
