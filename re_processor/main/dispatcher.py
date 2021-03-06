#!/usr/bin/env python
# coding=utf-8

import time, json, copy, os, uuid, shutil, math, zlib

from collections import defaultdict

import gevent

from re_processor.mixins.transceiver import BaseRabbitmqConsumer
from re_processor import settings
from re_processor.common import (
    debug_logger as logger, debug_info_logger, RedisLock,
    cache_rules, get_dev_rules_from_cache,
    set_monitor_data, get_monitor_dids, get_proc_t_info,
    get_rules_from_cache,
    getset_last_data,
    check_interval_locked,
    new_virtual_device_log, logstash_log, LogstashLogNode)
from re_processor.connections import get_mysql, get_redis
from re_processor.main.function import generate_msg_func_list, generate_func_list_msg, send_output_msg, custom_json

from processor import MainProcessor

from re_processor.common import log_debug_log

default_limit = {
    'msg_limit': settings.MSG_LIMIT,
    'triggle_limit': settings.TRIGGLE_LIMIT
}

class MainDispatcher(BaseRabbitmqConsumer):
    '''
    handle msgs like (device_online, device_offline, device_bind, device_unbind, device_status_kv, attr_fault, attr_alert)
    '''

    def __init__(self, mq_queue_name, product_key=None, routing_key=None):
        self.product_key_set = set()
        self.limit_dict = {}
        self.mq_queue_name = mq_queue_name
        self.product_key = product_key or '*'
        self.routing_key = routing_key or None
        self.mq_initial()
        self.processor = MainProcessor(MainSender(self.product_key))
        self.thermal_map = defaultdict(int)
        self.thermal_data = {}
        self.cache = get_redis()

    def save_thermal_data(self):
        obj_ids = filter(lambda x: self.thermal_map[x] > settings.THERMAL_THRESHOLD, self.thermal_map.keys())
        self.thermal_data = {x: get_dev_rules_from_cache(x) for x in obj_ids}
        self.thermal_map = defaultdict(int)

    def init_rules_cache(self):
        with RedisLock('re_core_product_key_set') as lock:
            if lock:
                db = get_mysql()
                cache = get_redis()
                p = cache.pipeline()
                cache_rule = defaultdict(list)
                pk_set = []

                # 获取所有限制
                sql = 'select `product_key`, `msg_limit`, `triggle_limit` from `{0}`'.format(
                    settings.MYSQL_TABLE['limit']['table'])
                db.execute(sql)
                result = db.fetchall()
                limit_dict = {x[0]: {'msg_limit': x[1], 'triggle_limit': x[2]} for x in result}
                p.set('re_core_rule_limit_dict', zlib.compress(json.dumps(limit_dict)))

                # 遍历所有规则
                id_max = 0
                while True:
                    sql = 'select `id`, `product_key`, `rule_tree`, `custom_vars`, `enabled`, `ver`, `type`, `interval`, `obj_id`, `params` from `{0}` where `id`>{1} order by `id` limit 500'.format(
                        settings.MYSQL_TABLE['rule']['table'],
                        id_max)
                    db.execute(sql)
                    result = db.fetchall()
                    if not result:
                        break

                    for rule_id, product_key, rule_tree, custom_vars, enabled, ver, type, interval, obj_id, params in result:
                        pk_set.append(product_key)
                        self.product_key_set.add(product_key)
                        if 1 != enabled:
                            continue
                        rule_tree = json.loads(rule_tree) if rule_tree else []
                        custom_vars = json.loads(custom_vars) if custom_vars else {}

                        cache_rule[obj_id].append({
                            'ver': ver,
                            'rule_id': rule_id,
                            'rule_tree': rule_tree,
                            'custom_vars': custom_vars,
                            'params': json.loads(params) if params else [],
                            'type': type,
                            'interval': interval
                        })

                    id_max = result[-1][0]

                if pk_set:
                    p.sadd('re_core_product_key_set', *pk_set)

                cache_rules(cache_rule)

                p.setnx('re_core_rule_cache_update', 1)
                p.expire('re_core_rule_cache_update', 82800)
                p.execute()

        return True

    def consume(self, ch, method, properties, body):
        log = {
            'ts': time.time(),
            'module': 're_processor_status',
            'running_status': 'beginning'
        }
        try:
            #print body
            msg = json.loads(body)
            if self.mq_queue_name == 'data' and msg['product_key'] in settings.PRODUCT_WHITELIST:
                logger.info("pk:{} in white list".format(msg['product_key']))
                if not settings.IS_NO_ACK:
                    self.channel.basic_ack(delivery_tag=method.delivery_tag)
                return
            msg['d3_limit'] = self.limit_dict.get(msg['product_key'], default_limit)
            if settings.IS_USE_GEVENT:
                gevent.spawn(self.dispatch, msg, method.delivery_tag, log)
            else:
                self.dispatch(msg, method.delivery_tag, log)
        except Exception, e:
            logger.exception(e)
            if not settings.IS_NO_ACK:
                self.channel.basic_ack(delivery_tag=method.delivery_tag)

    def dispatch(self, msg, delivery_tag, log):
        try:
            msg['logstash_msgid'] = uuid.uuid4().hex
            logstash_log(LogstashLogNode.ENTER_RE, 'msg enter', extra={
                'event_list': ['msg_enter_re_processor',],
                'product_key': msg['product_key'],
                'did': msg['did'],
                'mac': msg['mac'],
                'source': 'gw_re_processor',
                'node': settings.LOGSTASH_NODE,
                'msg_enter_re_processor.time_spent': time.time() - log['ts'],
                'logstash_msgid': msg['logstash_msgid'],
            })
            start_ts = time.time()
            thermal_data = self.thermal_data.get(msg['product_key'])
            pk_set = self.cache.smembers('re_core_product_key_set')
            if msg['product_key'] not in pk_set:
                return
            if thermal_data:
                rules_list = thermal_data + get_dev_rules_from_cache(msg['did'])
                is_thermal_data = 1
            else:
                rules_list = get_rules_from_cache(msg['product_key'], msg['did'])
                is_thermal_data = 0
            resp_t = get_proc_t_info(start_ts)
            data = msg.get('data', {})
            last_data = None
            for rule in rules_list:
                logstash_log(LogstashLogNode.RULE_READY, 'rule ready to process', extra={
                    'event_list': ['rule_ready_to_process',],
                    'product_key': msg['product_key'],
                    'did': msg['did'],
                    'mac': msg['mac'],
                    'source': 'gw_re_processor',
                    'node': settings.LOGSTASH_NODE,
                    'rule_ready_to_process.time_spent': time.time() - log['ts'],
                    'rule_id': rule['rule_id'],
                    'logstash_msgid': msg['logstash_msgid'],
                })
                p_log = {
                    'module': 're_processor',
                    'rule_id': rule['rule_id'],
                    'event': msg.get('event_type'),
                    'product_key': msg['product_key'],
                    'did': msg['did'],
                    'mac': msg['mac'],
                    'rule_type': rule['type'],
                    'interval': rule['interval'],
                    'current': 'log',
                    'is_thermal_data': is_thermal_data,
                    'ts': log['ts'],
                }
                if rule.get('ver') == 3:
                    log_debug_log(msg['logstash_msgid'], 'processing v3 rule. Before generate_msg_func_list')
                    task_info = generate_msg_func_list(rule, msg, last_data)
                    log_debug_log(msg['logstash_msgid'], 'After generate_msg_func_list')
                    task_obj = task_info[0]
                    dp_value = msg.get('data', {})
                    input_list = task_info[1]
                    output_wires = task_info[2]
                    log_debug_log(msg['logstash_msgid'], 'input_list len: {}'.format(len(input_list)))
                    for inp_index, inp in enumerate(input_list):
                        log_debug_log(msg['logstash_msgid'], 'processing input list. index: {}'.format(inp_index))
                        if inp['category'] != 'input':
                            continue
                        content = inp.get('content', {})
                        log_debug_log(msg['logstash_msgid'], 'Before new_virtual_device_log')
                        log_id = new_virtual_device_log(msg['product_key'], rule['rule_id']) if 'virtual:site' == msg['mac'] else ''
                        log_debug_log(msg['logstash_msgid'], 'After new_virtual_device_log')
                        task_vars = {}
                        if content.get('data_type') == 'custom':
                            log_debug_log(msg['logstash_msgid'], 'custom json data')
                            custom_info = custom_json(inp)
                            task_vars.update(custom_info)
                        log_debug_log(msg['logstash_msgid'], 'processing inp[wires][0]. len: {}'.format())
                        for inp_wires_index, inp_wires in enumerate(inp['wires'][0]):
                            log_debug_log(msg['logstash_msgid'], 'process wires. index: {}'.format(inp_wires_index))
                            log_debug_log(msg['logstash_msgid'], 'Before generate_func_list_msg')
                            data = generate_func_list_msg(task_obj, inp_wires, dp_value, output_wires, task_vars, log_id, msg)
                            log_debug_log(msg['logstash_msgid'], 'After generate_func_list_msg')
                            if data:
                                log_debug_log(msg['logstash_msgid'], 'After generate_func_list_msg processing data')
                                for d in data:
                                    if task_obj[d]['category'] == 'output':
                                        log_debug_log(msg['logstash_msgid'], 'Before send_output_msg')
                                        send_output_msg(task_obj[d], msg, p_log, task_vars, log_id, rule.get('rule_id'), p_log)
                                        log_debug_log(msg['logstash_msgid'], 'After send_output_msg')
                    p_log.update({
                        'proc_t': (time.time() - log['ts']) * 1000
                    })
                    logger.info(p_log)
            log_debug_log(msg['logstash_msgid'], 'Before mq_unpack')
            lst = self.mq_unpack(msg, log)
            map(lambda x: self.process(x, copy.deepcopy(log), logstash_msgid=msg['logstash_msgid']), lst)
            if settings.USE_DEBUG:
                resp_t = get_proc_t_info(start_ts)
                debug_info_logger.info("pk:{} process func use:{} ms".format(msg['product_key'], resp_t))
            # if msg['did'] in get_monitor_dids():
            #     proc_t = (time.time() - log['ts']) * 1000
            #     set_monitor_data('did:{}:resp_t'.format(msg['did']), proc_t, 3600)
        except Exception, e:
            logger.exception(e)
            log_debug_log(msg['logstash_msgid'], 'Here is dispatch exception. detail: {}'.format(str(e)))
        finally:
            if not settings.IS_NO_ACK:
                self.channel.basic_ack(delivery_tag=delivery_tag)

    def process(self, msg, log, logstash_msgid=''):
        try:
            self.processor.process_msg(msg, log, logstash_msgid)
        except Exception, e:
            logger.exception(e)
            log['exception'] = str(e)
            log['proc_t'] = int((time.time() - log['ts']) * 1000)
            logger.info(json.dumps(log))

    def begin(self):
        cache = get_redis()
        while True:
            try:
                pk_set = cache.smembers('re_core_product_key_set')
                if pk_set:
                    self.product_key_set = pk_set
                    break
            except:
                pass
            time.sleep(1)

        while True:
            try:
                limit_dict = cache.get('re_core_rule_limit_dict')
                if limit_dict:
                    self.limit_dict = json.loads(zlib.decompress(limit_dict))
                    break
            except:
                pass
            time.sleep(1)
        self.mq_listen(self.mq_queue_name, self.product_key, settings.IS_NO_ACK, self.routing_key)

    def update_rule_limit(self, update_list):
        limit_dict = {}
        with RedisLock('re_core_rule_limit_update_set') as lock:
            if lock:
                db = get_mysql()
                cache = get_redis()
                p = cache.pipeline()
                p.srem('re_core_rule_limit_update_set', *update_list)
                p.get('re_core_rule_limit_dict')
                res = p.execute()
                limit_dict = json.loads(zlib.decompress(res[1]))

                sql = 'select `product_key`, `msg_limit`, `triggle_limit` from `{0}` where `product_key` in ({1})'.format(
                    settings.MYSQL_TABLE['limit']['table'],
                    ','.join(['"{}"'.format(x) for x in update_list]))
                db.execute(sql)
                result = db.fetchall()

                update_limit = {x[0]: {'msg_limit': x[1], 'triggle_limit': x[2]} for x in result}
                limit_dict.update(update_limit)
                map(lambda x: limit_dict.pop(x) if x not in update_limit and x in limit_dict else None, update_list)

                cache.set('re_core_rule_limit_dict', zlib.compress(json.dumps(limit_dict)))

        return limit_dict


    def update_product_key_set(self):
        cache = get_redis()
        p = cache.pipeline()
        while True:
            try:
                if 'all' == self.mq_queue_name:
                    p.get('re_core_rule_cache_update')
                    p.smembers('re_core_rule_limit_update_set')
                    f_res = p.execute()
                    if not f_res[0]:
                        self.init_rules_cache()

                    if f_res[1]:
                        limit_dict = self.update_rule_limit(list(f_res[1]))
                        if limit_dict:
                            self.limit_dict = limit_dict

                p.smembers('re_core_product_key_set')
                p.get('re_core_rule_limit_dict')
                res = p.execute()
                if res[0]:
                    self.product_key_set = res[0]

                if res[1]:
                    self.limit_dict = json.loads(zlib.decompress(res[1]))

                self.save_thermal_data()
            except:
                pass
            finally:
                time.sleep(60)


class MainSender(BaseRabbitmqConsumer):

    def __init__(self, product_key=None):
        self.product_key = product_key or '*'
        self.mq_initial()

    def send(self, msg, product_key=None):
        self.mq_publish(product_key or self.product_key, [msg])

    def send_msgs(self, msgs):
        self.mq_publish(self.product_key, msgs)


# ToDo
class DeviceScheduleScanner(object):
    '''
    dispatch msgs ready to be dispatched
    '''

    def __init__(self, product_key=None):
        self.sender = MainSender(product_key)

        fid_name = '{}/fid'.format(settings.SCHEDULE_FILE_DIR.rstrip('/'))
        while True:
            if os.path.isfile(fid_name):
                with open(fid_name) as fp:
                    self.fid = fp.read()
                break
            else:
                with RedisLock('re_core_fid') as lock:
                    if lock:
                        with open(fid_name, 'w+') as fp:
                            self.fid = str(uuid.uuid4())
                            fp.write(self.fid)
                        break

            print 'waiting for fid file...'
            time.sleep(1)

        #print self.fid
        self.update_start_time()

    def update_start_time(self):
        try:
            self.start_time = reduce(lambda x, f_lst: min([x]+map(lambda y: int(y.split('_', 5)[-2]), f_lst[2])),
                                     os.walk('{}/task'.format(settings.SCHEDULE_FILE_DIR.rstrip('/'))),
                                     int(time.time()))
        except:
            self.start_time = int(time.time())

    def begin(self):
        cnt = 1
        while True:
            try:
                time_now = time.time()
                log = {
                    'module': 're_processor',
                    'handling': 'scan',
                    'running_status': 'dispatching',
                    'ts': time_now
                }
                ts = int(math.ceil(time_now))
                if time_now > self.start_time:
                    map(lambda x: gevent.spawn(self.scan, x, dict(log, ts=time_now)), xrange(self.start_time, ts+1))
                sleep_remain = ts - time.time()
                if sleep_remain > 0:
                    time.sleep(sleep_remain)
                self.start_time = ts
                cnt += 1
                if cnt > 60:
                    self.update_start_time()
                    cnt = 1
            except Exception, e:
                logger.exception(e)
                log['exception'] = str(e)
                log['proc_t'] = int((time.time() - log['ts']) * 1000)
                logger.info(json.dumps(log))

    def scan(self, ts, log):
        log['running_status'] = 'scan_device'
        minute = ts % 60
        dir_name = '{0}/task/{1}/{2}'.format(
            settings.SCHEDULE_FILE_DIR.rstrip('/'),
            minute,
            ts)
        if os.path.isdir(dir_name):
            cache = get_redis()
            lock_key = 're_core_{0}_{1}'.format(self.fid, ts)
            with RedisLock(lock_key) as lock:
                if lock and os.path.isdir(dir_name):
                    msg_lsit = reduce(lambda m_lst, f_lst: m_lst + \
                                      filter(lambda m: m, map(lambda x: self.read_msg('{0}/{1}'.format(f_lst[0], x)), f_lst[2])),
                                      os.walk(dir_name),
                                      [])

                    if msg_lsit:
                        self.sender.send_msgs(msg_lsit)
                    shutil.rmtree(dir_name)
                    cache.delete(lock_key)
                    log['dir_name'] = dir_name
                    logger.info(json.dumps(log))

    def read_msg(self, file_name):
        if not os.path.isfile(file_name):
            return {}

        msg = {}
        with open(file_name) as fp:
            msg = json.load(fp)

        return msg
