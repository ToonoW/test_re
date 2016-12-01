#!/usr/bin/env python
# coding=utf-8

import logging, json, requests, redis, copy
import logging.config

from re_processor import settings
from re_processor.connections import get_redis, get_mysql

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
        cache = get_redis()
        p = cache.pipeline()
        p.lpush(key, json.dumps(data))
        p.expire(key, expire)
        p.ltrim(key, 0, settings.SEQUENCE_MAX_LEN-1)
        p.execute()
    except redis.exceptions.RedisError, e:
        return {'error_message': 'redis error: {}'.format(str(e))}
    else:
        return True

def update_several_sequence(data, expire=settings.SEQUENCE_EXPIRE):
    try:
        cache = get_redis()
        p = cache.pipeline()
        for key, val in data.items():
            p.lpush(key, json.dumps(val))
            p.expire(key, expire)
            p.ltrim(key, 0, settings.SEQUENCE_MAX_LEN-1)
        p.execute()
    except redis.exceptions.RedisError, e:
        return {'error_message': 'redis error: {}'.format(str(e))}
    else:
        return True

def get_sequence(key, length, start=0):
    try:
        cache = get_redis()
        result = cache.lrange(key, start, start+length-1) or []
        if result:
            result = map(lambda x: json.loads(x), result)
            res_len = len(result)
            if res_len < length:
                result.extend([copy.deepcopy(result[-1])] * (length - res_len))
            else:
                result = result[:length]
    except redis.exceptions.RedisError, e:
        result = {'error_message': 'redis error: {}'.format(str(e))}

    return result

def update_device_status(product_key, did, mac, status, ts):
    db = get_mysql()
    sql = 'select 1 from `{0}` where `did`="{1}" and `mac`="{2}" limit 1'.format(
        settings.MYSQL_TABLE['device_status']['table'],
        did,
        mac)
    db.execute(sql)
    if db.fetchall():
        sql = 'update `{0}` set `is_online`={1}, `ts`="{2}" where `did`="{3}" and `mac`="{4}" limit 1'.format(
            settings.MYSQL_TABLE['device_status']['table'],
            status,
            str(ts),
            did,
            mac)
    else:
        sql = 'insert into `{0}` set `product_key`="{1}", `did`="{2}", `mac`="{3}", `is_online`={4}, `ts`="{5}"'.format(
            settings.MYSQL_TABLE['device_status']['table'],
            product_key,
            did,
            mac,
            status,
            str(ts))
    db.execute(sql)
    db.close()
