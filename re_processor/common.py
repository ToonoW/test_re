#!/usr/bin/env python
# coding=utf-8

import logging, json, requests, redis, copy
import logging.config

from re_processor import settings
from re_processor.connections import redis_pool

logging.config.dictConfig(settings.LOGGING)

logger = logging.getLogger('processor_gray')
debug_logger = logging.getLogger('debug_gray')
console_logger = logging.getLogger('processor')

def _log(log):
    logger.info(json.dumps(log))

def debug_log(log):
    debug_logger.info(json.dumps(log))

def new_virtual_device_log(product_key, rule_id):
    url = 'http://{}/log'.format(settings.REAPI_HOST)
    headers = {
        'X-Gizwits-Rulesengine-Token': settings.REAPI_TOKEN
    }
    data = {
        'product_key': product_key,
        'rule_id': rule_id
    }
    log_id = ''
    try:
        resp = requests.post(url, data=json.dumps(data), headers=headers)
        if 201 == resp.status_code:
            log_id = json.loads(resp.content)['log_id']
    except Exception, e:
        logger.warning(str(e))

    return log_id

def update_virtual_device_log(log_id, field, value, exception=''):
    if not log_id:
        return None
    url = 'http://{}/log'.format(settings.REAPI_HOST)
    headers = {
        'X-Gizwits-Rulesengine-Token': settings.REAPI_TOKEN
    }
    data = {
        'log_id': log_id,
        'field': field,
        'value': value,
        'exception': exception
    }

    try:
        requests.put(url, data=json.dumps(data), headers=headers)
    except Exception, e:
        logger.warning(str(e))

    return log_id

def update_sequence(key, data, expire=settings.SEQUENCE_EXPIRE):
    try:
        cache = redis.Redis(connection_pool=redis_pool)
        p = cache.pipeline()
        p.lpush(key, data)
        p.expire(key, expire)
        p.execute()
    except redis.exceptions.RedisError, e:
        return {'error_message': 'redis error: {}'.format(str(e))}
    else:
        return True

def get_sequence(key, start, length):
    try:
        cache = redis.Redis(connection_pool=redis_pool)
        result = cache.lrange(start, start+length-1)
        if result:
            res_len = len(result)
            if res_len < length:
                result.extend([copy.deepcopy(result[0])] * (length - res_len))
            else:
                result = result[:length]
        else:
            result = []
    except redis.exceptions.RedisError, e:
        result = {'error_message': 'redis error: {}'.format(str(e))}

    return result
