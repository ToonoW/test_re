#!/usr/bin/env python
# coding=utf-8

import json, operator, re, time, copy, requests, redis

from re_processor import settings
from re_processor.connections import get_mongodb, get_mysql, redis_pool
from re_processor.common import _log


class BaseCore(object):
    """BaseCore"""

    def process(self, msg):
        '''
        execute task, return a three-tuple (result, msg_list, log_flag)
        '''


class InputCore(BaseCore):
    """InputCore"""



class FuncCore(BaseCore):
    """FuncCore"""



class OutputCore(BaseCore):
    """OutputCore"""
