#!/usr/bin/env python
# coding=utf-8

import time, json

from re_processor.mixins import core as core_mixins
from re_processor import settings
from re_processor.common import debug_logger as logger, update_virtual_device_log, set_interval_lock
from re_processor.common import (
    check_rule_limit, _log,
    set_device_offline_ts,
    get_device_offline_ts,
    clean_device_offline_ts
)
from re_processor.celery import delay_sender
from re_processor.connections import get_mysql


log_status = {
    'success': 1,
    'failed': 2,
    'exception': 3
}


def get_notification_product_interval(product_key):
    """
    读取特殊pk延时设置信息
    """
    db = get_mysql()
    sql = "select `interval` from `{0}` where `product_key`='{1}'".format(
        settings.MYSQL_TABLE['product_delay_setting']['table'], product_key)
    db.execute(sql)
    result = db.fetchone()
    if not result:
        return False
    return result[0]


class MainProcessor(object):
    '''
    main processor
    '''

    def __init__(self, sender):
        self.core = {}
        for i in [1,3]:
            core_map = settings.CORE_MAP.get('v{}'.format(i), None)
            if core_map is None:
                raise Exception(u'start processor failed: error version "{}"'.format(i))
            self.core[i] = {k: getattr(core_mixins, v)() for k, v in core_map.items()} if core_map else None
        self.sender = sender

    def notification_sender(self, delay_time, msg, product_key, did, ts):
        """
        对notification特殊pk进行延时推送设置
        """
        event = msg.get('event', '')
        if get_device_offline_ts(did) and event == 'device_online':
            clean_device_offline_ts(did)
            return
        if not get_device_offline_ts(did) and event == 'device_online':
            self.sender.send(msg, product_key)
        if event == 'device_offline':
            set_device_offline_ts(did, ts, delay_time)
            delay_sender.apply_async(args=(msg, product_key), countdown=delay_time)

    def process_msg(self, src_msg, log={}):
        '''
        return a list of msg
        '''
        log['running_status'] = 'process'
        product_key = src_msg['task_vars'].get('product_key', '')
        did = src_msg['task_vars'].get('did', '')
        mac = src_msg['task_vars'].get('mac', '')
        msg_list = [src_msg]
        task_type = 'type_error'
        error_message = ''
        result = False
        ts = log.get('ts', time.time())
        p_log = {
            'module': 're_processor',
            'rule_id': src_msg.get('rule_id', ''),
            'event': src_msg.get('event', ''),
            'product_key': product_key,
            'did': did,
            'mac': mac,
            'rule_type': src_msg.get('type', 1),
            'interval': src_msg.get('interval', 0),
            'current': 'log',
            'ts': ts,
        }

        while msg_list:
            msg = msg_list.pop(0)
            try:
                if settings.MSG_TO['external'] == msg['msg_to']:
                    delay_time = get_notification_product_interval(product_key)
                    action_type = msg.get('action_type', '')
                    event = msg.get('event', '')
                    if 3 == src_msg['ver']:
                        if check_rule_limit(product_key, src_msg['task_vars']['d3_limit']['triggle_limit'], 'triggle'):
                            if delay_time and action_type == 'notification' and event in ['device_online', 'device_offline']: # 若为消息推送，则离线数据延时推送
                                self.notification_sender(delay_time, msg, product_key, did, ts)
                            else:
                                self.sender.send(msg, product_key)
                        else:
                            _log(dict(p_log,
                                result='failed',
                                proc_t=((time.time() - ts) * 1000),
                                handling='action',
                                error_message='quota was used up'
                            ))
                    else:
                        if delay_time and action_type == 'notification' and event in ['device_online', 'device_offline']: # 若为消息推送，则离线数据延时推送
                            self.notification_sender(delay_time, msg, product_key, did, ts)
                        else:
                            self.sender.send(msg, product_key)
                    continue
                task_type = msg['current']['category'] if 3 == msg['ver'] else msg['current']
                _result, _msg_list = self.core[msg['ver']][task_type].process(msg)
                msg_list.extend(_msg_list)
            except Exception, e:
                _result = 'exception'
                error_message = str(e)
                logger.exception(e)

            if 'exception' == _result:
                _log(dict(p_log,
                    result=_result,
                    proc_t=((time.time() - ts) * 1000),
                    handling=('action' if 'tri' == task_type or 'output' == task_type else 'rule'),
                    extern_params=msg.get('extern_params', ''),
                    task_vars=msg.get('task_vars'),
                    task_type=(msg['current']['type'] if 3 == msg['ver'] else task_type),
                    error_message=(error_message or msg.get('error_message', ''))
                ))

            if 'tri' == task_type or 'output' == task_type:
                result = result or _result

        if 'exception' != result:
            result = 'success' if result else 'failed'
            p_log.update(
                result=result,
                proc_t=((time.time() - ts) * 1000),
                handling=('action' if 'tri' == task_type or 'output' == task_type else 'rule')
            )
            if 'action' == p_log['handling'] and 'success' == result and p_log['interval'] > 0:
                set_interval_lock(p_log['rule_id'], p_log['did'], p_log['interval'])

            if settings.DEBUG is True or 'virtual:site' == mac or 'exception' == result:
                p_log['extern_params'] = src_msg.get('extern_params', '')
                p_log['task_vars'] = src_msg['task_vars']
                p_log['task_type'] = src_msg['current']['type'] if 3 == src_msg['ver'] else task_type
                p_log['error_message'] = error_message or src_msg.get('error_message', '')

            _log(p_log)

        if 'virtual:site' == src_msg['task_vars'].get('mac', '') and ('success' != result or 'action' == p_log['handling']):
            update_virtual_device_log(src_msg.get('log_id'), 'triggle', log_status[result], error_message)

        return msg_list
