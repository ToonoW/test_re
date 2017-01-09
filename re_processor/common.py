#!/usr/bin/env python
# coding=utf-8

import logging, json, requests, redis, copy, zlib
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

def cache_rules(rules, product_key=None):
    if rules:
        try:
            cache = get_redis()
            p = cache.pipeline()
            if product_key:
                p.sadd('re_core_product_key_set', product_key)
            for k, v in rules.items():
                if v:
                    p.set('re_core_{}_cache_rules'.format(k), zlib.compress(json.dumps(v)))
                    p.expire('re_core_{}_cache_rules'.format(k), 86400)
                else:
                    p.delete('re_core_{}_cache_rules'.format(k))
            p.execute()
        except redis.exceptions.RedisError:
            pass

def get_rules_from_cache(product_key, did):
    cache = get_redis()
    p = cache.pipeline()
    p.get('re_core_{}_cache_rules'.format(product_key))
    p.get('re_core_{}_cache_rules'.format(did))
    result = p.execute()
    return reduce(lambda rules, x: rules + (json.loads(zlib.decompress(x)) if x else []), result, [])

def getset_last_data(data, did):
    cache = get_redis()
    p = cache.pipeline()
    p.getset('re_core_{}_dev_latest'.format(did), zlib.compress(json.dumps(data)))
    p.expire('re_core_{}_dev_latest'.format(did), 86400)
    result = p.execute()
    return json.loads(zlib.decompress(result[0])) if result[0] else {}

def set_interval_lock(rule_id, did, interval):
    if not rule_id or not interval:
        return {}
    lock = False
    result = {}
    try:
        cache = get_redis()
        lock = cache.setnx('re_core_{0}_{1}_rule_interval'.format(did, rule_id), 1)
    except redis.exceptions.RedisError, e:
        result = {'error_message': 'redis error: {}'.format(str(e))}
    except Exception, e:
        result = {'error_message': 'error: {}'.format(str(e))}
    finally:
        if lock:
            cache.expire('re_core_{0}_{1}_rule_interval'.format(did, rule_id), interval)

    return result

def check_interval_locked(rule_id, did):
    try:
        cache = get_redis()
        lock = cache.get('re_core_{0}_{1}_rule_interval'.format(did, rule_id))
        return bool(lock)
    except redis.exceptions.RedisError:
        return False


class RedisLock(object):

    def __init__(self, cache_key):
        self.cache_key = cache_key

    def __enter__(self):
        self.cache = get_redis()
        self.lock = self.cache.setnx(self.cache_key+'_lock', 1)
        return self.lock

    def __exit__(self, type, value, traceback):
        if self.lock:
            self.cache.delete(self.cache_key+'_lock')
        return False
