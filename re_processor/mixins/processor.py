#!/usr/bin/env python
# coding=utf-8

import time

from re_processor.mixins import core as core_mixins
from re_processor import settings


class CommonProcessor(object):
    '''
    common processor
    '''

    def processor_initial(self, pos=None):
        core_map = settings.CORE_MAP.get(pos, None)
        self.core = {k: getattr(core_mixins, v)() for k, v in core_map.items()} if core_map else None

    def process_msg(self, msg, log=None):
        '''
        return a list of msg
        '''

        if not getattr(self, 'core', None):
            return [msg]
        msg_list = []
        p_log = []
        ts = time.time()
        task_type = 'type_error'
        error_message = ''
        try:
            task_type = msg['current']
            result, msg_list, log_flag = self.core[task_type].process(msg)
        except Exception, e:
            log_flag = True
            result = 'exception'
            error_message = e.message

        if log_flag:
            p_log = {
                'result': result,
                'ts': ts,
                'proc_t': (time.time() - ts) * 1000,
                'error_message': error_message,
                'task_type': task_type
            }
            p_log.update(msg)
            p_log['current'] = 'log'
            msg_list.append(p_log)

        return msg_list