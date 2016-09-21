#!/usr/bin/env python
# coding=utf-8

import time, json

from re_processor.mixins import core as core_mixins
from re_processor import settings
from re_processor.common import debug_logger as logger
from re_processor.common import _log


class MainProcessor(object):
    '''
    main processor
    '''

    def __init__(self, ver, sender):
        core_map = settings.CORE_MAP.get('v{}'.format(ver), None)
        if core_map is None:
            raise Exception(u'start processor failed: error version "{}"'.format(ver))
        self.core = {k: getattr(core_mixins, v)() for k, v in core_map.items()} if core_map else None
        self.sender = sender

    def process_msg(self, src_msg, log={}):
        '''
        return a list of msg
        '''

        log['running_status'] = 'process'
        msg_list = [src_msg]
        task_type = 'type_error'
        error_message = ''
        while msg_list:
            msg = msg_list.pop(0)
            try:
                if settings.MSG_TO['external'] == msg['msg_to']:
                    self.sender.send(msg)
                    continue
                task_type = msg['current']
                result, _msg_list, log_flag = self.core[task_type].process(msg)
                msg_list.extend(_msg_list)
            except Exception, e:
                log_flag = True
                result = 'exception'
                error_message = str(e)
                logger.exception(e)
            if log_flag:
                if 'exception' != result:
                    result = 'success' if result else 'failed'
                ts = msg.get('ts', time.time())
                p_log = {
                    'msg_to': settings.MSG_TO['internal'],
                    'module': 're_processor',
                    'rule_id': msg.get('rule_id', ''),
                    'action_id_list': ','.join(msg.get('action_id_list', [])),
                    'event': msg.get('event', ''),
                    'product_key': msg['task_vars'].get('product_key', ''),
                    'did': msg['task_vars'].get('did', ''),
                    'mac': msg['task_vars'].get('mac', ''),
                    'extern_params': msg.get('extern_params', ''),
                    'task_vars': json.dumps(msg['task_vars']),
                    'current': 'log',
                    'result': result,
                    'ts': ts,
                    'proc_t': (time.time() - ts) * 1000,
                    'error_message': error_message,
                    'task_type': task_type,
                    'handling': 'action' if 'tri' == task_type else 'rule'
                }
                _log(p_log)

        return msg_list
