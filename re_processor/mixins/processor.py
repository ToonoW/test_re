#!/usr/bin/env python
# coding=utf-8

import time

from re_processor.mixins import core as core_mixins
from re_processor import settings


class InternalProcessor(object):
    '''
    Internal processor
    '''

    def processor_initial(self):
        self.core = {k: getattr(core_mixins, v)() for k, v in settings.CORE_INTERNAL.items()}

    def process_msg(self, msg, log=None):
        '''
        return a list of msg
        '''
        p_log = []
        ts = time.time()
        type = 'type_error'
        try:
            type = msg['current']
            result, msg['task_vars'], msg['task_list'] = self.core[type].process(
                msg['task_list'], msg['task_vars'], msg['custom_vars']
                )
            p_log = [result, ts, (time.time() - ts) * 1000, '', type]
        except Exception, e:
            p_log = ['Exception', ts, (time.time() - ts) * 1000, e.message, type]
            result = False

        msg['log'].append(p_log)
        if result:
            msg['current'] = msg['task_list'][0][0] if msg['task_list'] else 'tri'
        else:
            msg['current'] = 'log'

        return msg


class OutputProcessor(object):
    '''
    Output processor
    '''

    def processor_initial(self):
        self.core = {k: getattr(core_mixins, v)() for k, v in settings.CORE_OUTPUT.items()}

    def process_msg(self, msg, log=None):
        p_log = []
        ts = time.time()
        type = 'type_error'
        try:
            type = msg['current']
            result, output_msg = self.core[type].process(
                msg['task_list'], msg['task_vars'], msg['custom_vars']
                )
            p_log = [result, ts, (time.time() - ts) * 1000, '', type]
        except Exception, e:
            p_log = ['Exception', ts, (time.time() - ts) * 1000, e.message, type]
            result = False

        msg['log'].append(p_log)
        msg['current'] = 'log'

        return msg


class InputProcessor(object):
    '''
    Input processor
    '''

    def processor_initial(self):
        self.core = None

    def process_msg(self, msg, log=None):
        return msg
