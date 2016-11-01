#!/usr/bin/env python
# coding=utf-8

import logging, json, requests
import logging.config

from re_processor import settings

logging.config.dictConfig(settings.LOGGING)

logger = logging.getLogger('processor_gray')
debug_logger = logging.getLogger('debug_gray')
console_logger = logging.getLogger('processor')

def _log(log):
    logger.info(json.dumps(log))

def debug_log(log):
    debug_logger.info(json.dumps(log))

def new_virtual_device_log(product_key, rule_id):
    pass

def update_virtual_device_log(log_id, field, value):
    pass
