#!/usr/bin/env python
# coding=utf-8

import time, json, copy, os, uuid, shutil, math

from collections import defaultdict

import gevent

from re_processor.mixins.transceiver import BaseRabbitmqConsumer
from re_processor import settings
from re_processor.common import debug_logger as logger, console_logger, RedisLock, cache_rules
from re_processor.connections import get_mysql, get_redis

from processor import MainProcessor

class MainDispatcher(BaseRabbitmqConsumer):
    '''
    handle msgs like (device_online, device_offline, device_bind, device_unbind, device_status_kv, attr_fault, attr_alert)
    '''

    def __init__(self, mq_queue_name, product_key=None):
        self.product_key_set = set([])
        self.mq_queue_name = mq_queue_name
        self.product_key = product_key or '*'
        self.mq_initial()
        self.processor = MainProcessor(MainSender(self.product_key))

    def init_rules_cache(self):
        db = get_mysql()
        id_max = 0
        cache = get_redis()
        cache.delete('re_core_product_key_set')

        while True:
            pk_set = set()
            cache_rule = defaultdict(list)
            sql = 'select `id`, `product_key`, `rule_tree`, `custom_vars`, `enabled`, `ver`, `type`, `interval`, `obj_id`, `params` from `{0}` where `id`>{1} order by `id` limit 100'.format(
                settings.MYSQL_TABLE['rule']['table'],
                id_max)
            db.execute(sql)
            result = db.fetchall()
            if not result:
                break

            for rule_id, product_key, rule_tree, custom_vars, enabled, ver, type, interval, obj_id, params in result:
                if 1 != enabled:
                    continue
                pk_set.add(product_key)
                self.product_key_set.add(product_key)
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

            cache.sadd('re_core_product_key_set', *list(pk_set))
            cache_rules(cache_rule)

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
            if msg['product_key'] in self.product_key_set or 'device_schedule' == msg['event_type']:
                gevent.spawn(self.dispatch, msg, log)
        except Exception, e:
            console_logger.exception(e)
            log['exception'] = str(e)
            log['proc_t'] = int((time.time() - log['ts']) * 1000)
            logger.info(json.dumps(log))

        #self.channel.basic_ack(delivery_tag=method.delivery_tag)

    def dispatch(self, msg, log):
        lst = self.mq_unpack(msg, log)
        if lst:
            map(lambda x: gevent.spawn(self.processor.process_msg, x, copy.deepcopy(log)), lst)

    def begin(self):
        cache = get_redis()
        try:
            pk_set = cache.smembers('re_core_product_key_set')
            if pk_set:
                self.product_key_set = pk_set
        except:
            pass
        self.mq_listen(self.mq_queue_name, self.product_key)

    def update_product_key_set(self):
        cache = get_redis()
        while True:
            try:
                pk_set = cache.smembers('re_core_product_key_set')
                if pk_set:
                    self.product_key_set = pk_set
            except:
                pass
            finally:
                time.sleep(60)


class MainSender(BaseRabbitmqConsumer):

    def __init__(self, product_key=None):
        self.product_key = product_key or '*'
        self.mq_initial()

    def send(self, msg):
        self.mq_publish(self.product_key, [msg])

    def send_msgs(self, msgs):
        self.mq_publish(self.product_key, msgs)


class ScheduleBufferConsumer(BaseRabbitmqConsumer):
    '''
    store msgs waiting to be dispatched
    '''

    def __init__(self, mq_queue_name, product_key=None):
        self.mq_queue_name = mq_queue_name
        self.product_key = product_key or '*'
        self.mq_initial()
        self.sender = MainSender(self.product_key)

    def consume(self, ch, method, properties, body):
        log = {
            'ts': time.time(),
            'handling': 'buffer',
            'module': 're_processor',
            'running_status': 'beginning'
        }
        try:
            #print body
            #gevent.spawn(self.waiting, body, log)
            self.waiting(body, log)
        except Exception, e:
            console_logger.exception(e)
            log['exception'] = str(e)
            log['proc_t'] = int((time.time() - log['ts']) * 1000)
        finally:
            logger.info(json.dumps(log))
            self.channel.basic_ack(delivery_tag=method.delivery_tag)

    def begin(self):
        self.mq_listen(self.mq_queue_name, self.product_key, no_ack=False)

    def waiting(self, body, log):
        '''
        msg:
        {
            "action_type": "schedule_wait",
            "product_key": <product_key string>,
            "did": <did string>,
            "mac": <mac string>,
            "rule_id": <rule_id int>,
            "node_id" <node_id string>,
            "ts": <timestamp_in_seconds int>,
            "flag": <timestamp_in_ms int>,
            "once": <is_once boolean>
        }
        '''
        log['running_status'] = 'waiting'
        msg = json.loads(body)
        #print msg

        minute = msg['ts'] % 60
        file_dir = '{0}/task/{1}/{2}/{3}'.format(
            settings.SCHEDULE_FILE_DIR.rstrip('/'),
            minute,
            msg['ts'],
            hash(msg.get('did') or msg['product_key']) % settings.DEVICE_HASH_GROUP
            )
        if not os.path.exists(file_dir):
            os.makedirs(file_dir, mode=0777)

        file_name = '{0}/{1}_{2}_{3}_{4}'.format(
            file_dir,
            msg.get('did', ''),
            msg['rule_id'],
            msg['node_id'],
            msg['flag']
            )
        if 'msg' in msg:
            s_msg = msg['msg']
        else:
            s_msg = {
                'msg_to': settings.MSG_TO['external'],
                'action_type': 'schedule',
                'event_type': 'device_schedule',
                'created_at': time.time(),
                'product_key': msg['product_key'],
                'did': msg['did'],
                'mac': msg['mac'],
                'rule_id': msg['rule_id'],
                'node_id': msg['node_id'],
                'flag': msg['flag'],
                'once': msg['once']
            }

        with open(file_name, 'w+') as fp:
            json.dump(s_msg, fp)

        log['schedule_msg'] = s_msg


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
                console_logger.exception(e)
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


class ProductScheduleScanner(object):
    '''
    dispatch msgs ready to be dispatched
    '''

    def __init__(self, product_key=None):
        self.sender = MainSender(product_key)

    def begin(self):
        while True:
            try:
                time_now = time.time()
                log = {
                    'module': 're_processor',
                    'handling': 'scan',
                    'running_status': 'dispatching',
                    'ts': time_now
                }
                min = int(time_now) / 60
                gevent.spawn(self.scan, min, dict(log, ts=time_now))
                sleep_remain = min * 60 + 60 - time.time()
                if sleep_remain > 0:
                    time.sleep(sleep_remain)
            except Exception, e:
                console_logger.exception(e)
                log['exception'] = str(e)
                log['proc_t'] = int((time.time() - log['ts']) * 1000)
            finally:
                logger.info(json.dumps(log))


    def scan(self, min, log):
        log['running_status'] = 'scan_product'
        db = get_mysql()
        update_sql = 'update `{0}` set `next`=`next`+`interval` where `id` in ({1})'
        limit = 100
        id_max = 0
        while True:
            sql = 'select `id`, `rule_id_id`, `node` from `{0}` where `id`>{1} and `next`<={2} order by `id` limit {3}'.format(
                settings.MYSQL_TABLE['schedule']['table'],
                id_max,
                min,
                limit)
            db.execute(sql)
            result = db.fetchall()
            if not result:
                break

            msg = {
                'action_type': 'schedule',
                'event_type': 'device_schedule',
                'created_at': log['ts'],
                'did': '',
                'mac': '',
                'flag': '',
                'once': True
            }
            msg_lsit = map(lambda res: dict(msg, rule_id=res[1], node_id=res[2]),
                           result)
            if msg_lsit:
                self.sender.send_msgs(msg_lsit)

            sql = update_sql.format(
                settings.MYSQL_TABLE['schedule']['table'],
                ','.join([str(x[0]) for x in result])
                )
            db.execute(sql)
            id_max = result[-1][0]

        db.close()
