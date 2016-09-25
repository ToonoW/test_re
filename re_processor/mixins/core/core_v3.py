#!/usr/bin/env python
# coding=utf-8

import json, operator, re, time, copy, requests, redis

from re_processor import settings
from re_processor.connections import get_mongodb, get_mysql, redis_pool
from re_processor.common import _log

from core_v1 import get_value_from_json, get_value_from_task


class BaseCore(object):
    """BaseCore"""

    def process(self, msg):
        '''
        execute task, return a three-tuple (result, msg_list, log_flag)
        '''
        msg_list, log_flag = getattr(self, msg['current']['type'])(msg)

        return (True if msg_list else False), msg_list, log_flag


class InputCore(BaseCore):
    """InputCore"""

    def device_data(self, msg):
        pass

    def custom_json(self, msg):
        pass


class FuncCore(BaseCore):
    """FuncCore"""

    def condition(self, msg):
        pass

    def calculator(self, msg):
        pass

    def script(self, msg):
        pass


class OutputCore(BaseCore):
    """OutputCore"""

    def notification(self, msg):
        pass

    def http(self, msg):
        pass

    def sms(self, msg):
        pass

    def email(self, msg):
        pass

    def devctrl(self, msg):
        pass

    def es(self, msg):
        pass
