#!/usr/bin/env python
# coding=utf-8

import logging, json
import logging.config

from re_processor import settings

logging.config.dictConfig(settings.LOGGING)

logger = logging.getLogger('processor_gray')
debug_logger = logging.getLogger('debug_gray')

def _log(log):
    logger.info(json.dumps(log))

def debug_log(log):
    debug_logger.info(json.dumps(log))
