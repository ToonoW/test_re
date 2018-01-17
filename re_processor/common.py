#!/usr/bin/env python
# coding=utf-8

import logging, json, requests, redis, copy, zlib, time
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
        logger.exception(e)

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
        logger.exception(e)

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
        logger.exception(e)
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
        logger.exception(e)
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
        logger.exception(e)
        result = {'error_message': 'redis error: {}'.format(str(e))}

    return result

def update_device_online(did, ts, status=False):
    cache = get_redis()
    key = 're_core_{}_dev_online_ts'.format(did)
    try:
        if status:
            cache.set(key , ts)
        else:
            cache.delete(key)
    except redis.exceptions.RedisError, e:
        logger.exception(e)

def check_device_online(did):
    cache = get_redis()
    try:
        result = cache.get('re_core_{}_dev_online_ts'.format(did))
    except redis.exceptions.RedisError, e:
        logger.exception(e)
        result = None

    return result

def set_schedule_msg(key, ts, now, msg):
    cache = get_redis()
    p = cache.pipeline()
    offset = ts%86400
    bit_key = 're_core_{}_schedule_bitmap'.format(ts-offset)
    _key = 're_core_{}_schedule_set'.format(ts)
    try:
        expire_sec = int(86400 + ts - now)
        p.sadd(_key, key)
        p.expire(_key, expire_sec)
        p.set(key, json.dumps(msg))
        p.expire(key, expire_sec)
        p.setbit(bit_key, offset, 1)
        p.execute()
    except redis.exceptions.RedisError, e:
        logger.exception(e)


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
        except redis.exceptions.RedisError, e:
            logger.exception(e)

def get_rules_from_cache(product_key, did):
    cache = get_redis()
    p = cache.pipeline()
    p.get('re_core_{}_cache_rules'.format(product_key))
    p.get('re_core_{}_cache_rules'.format(did))
    result = p.execute()
    return reduce(lambda rules, x: rules + (json.loads(zlib.decompress(x)) if x else []), result, [])

def get_dev_rules_from_cache(did):
    cache = get_redis()
    result = cache.get('re_core_{}_cache_rules'.format(did))
    return json.loads(zlib.decompress(result)) if result else []

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
        logger.exception(e)
        result = {'error_message': 'redis error: {}'.format(str(e))}
    except Exception, e:
        logger.exception(e)
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
    except redis.exceptions.RedisError, e:
        logger.exception(e)
        return False

def check_rule_limit(product_key, limit, type, incr=True):
    if not limit:
        return True

    cache = get_redis()
    key = 're_core_{0}_{1}_limit'.format(product_key, type)
    if incr:
        num = cache.incr(key)
        if 1 == num:
            cache.expire(key, int(time.mktime(time.strptime(time.strftime('%Y-%m-%d'), '%Y-%m-%d')) + 86400 - time.time()))
    else:
        num = cache.get(key) or 0
    return int(num) <= limit


def set_noti_product_interval(product_key, delay_time):
    '''
    设备 Notification 间隔时间缓存
    '''
    cache = get_redis()
    p = cache.pipeline()
    key = 're_core_product_{}_interval'.format(product_key)
    try:
        p.set(key, str(delay_time))
        p.expire(key, settings.NOTIFICATION_INTERVAL_EXPIRE)
        p.execute()
    except redis.exceptions.RedisError, e:
        logger.exception(e)


def get_noti_product_interval(product_key):
    '''
    获取间隔时间缓存
    '''
    try:
        cache = get_redis()
        key = 're_core_product_{}_interval'.format(product_key)
        return cache.get(key)
    except redis.exceptions.RedisError, e:
        logger.exception(e)
        return 0


def set_device_offline_ts(did, ts, interval):
    """
    设置设备离线发送时间
    """
    cache = get_redis()
    p = cache.pipeline()
    key = 're_device_{}_offline_ts'.format(did)
    try:
        p.set(key, str(ts))
        p.expire(key, int(interval) + 2)
        p.execute()
    except redis.exceptions.RedisError, e:
        logger.exception(e)


def set_device_online_count(did):
    cache = get_redis()
    key = 're_device_{}_online_count'.format(did)
    num = cache.incr(key)
    try:
        return cache.incr(key)
    except redis.exceptions.RedisError, e:
        logger.exception(e)
        return False

def get_device_online_count(did):
    try:
        cache = get_redis()
        key = 're_device_{}_online_count'.format(did)
        return cache.get(key)
    except redis.exceptions.RedisError, e:
        logger.exception(e)
        return False


def clean_device_online_count(did):
    try:
        cache = get_redis()
        key = 're_device_{}_online_count'.format(did)
        return cache.delete(key)
    except redis.exceptions.RedisError, e:
        logger.exception(e)
        return False


def get_device_offline_ts(did):
    """
    获取设备离线时间
    """
    try:
        cache = get_redis()
        key = 're_device_{}_offline_ts'.format(did)
        return cache.get(key)
    except redis.exceptions.RedisError, e:
        logger.exception(e)
        return False


def clean_device_offline_ts(did):
    try:
        cache = get_redis()
        key = 're_device_{}_offline_ts'.format(did)
        return cache.delete(key)
    except redis.exceptions.RedisError, e:
        logger.exception(e)
        return False

class RedisLock(object):

    def __init__(self, cache_key):
        self.cache_key = '{}_lock_1'.format(cache_key)

    def __enter__(self):
        self.cache = get_redis()
        self.lock = self.cache.setnx(self.cache_key, 1)
        if self.lock:
            self.cache.expire(self.cache_key, settings.REDIS_LOCK_EXPRIE)
        return self.lock

    def __exit__(self, type, value, traceback):
        if self.lock:
            self.cache.delete(self.cache_key)
        return False
