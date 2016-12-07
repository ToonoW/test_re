#!/usr/bin/env python
# coding=utf-8

import time, json, copy, os, uuid, shutil, math

import gevent

from re_processor.mixins.transceiver import BaseRabbitmqConsumer
from re_processor import settings
from re_processor.common import debug_logger as logger, console_logger
from re_processor.connections import get_mysql, get_redis

from processor import MainProcessor

class MainDispatcher(BaseRabbitmqConsumer):
    '''
    handle msgs like (device_online, device_offline, device_bind, device_unbind, device_status_kv, attr_fault, attr_alert)
    '''

    def __init__(self, mq_queue_name, product_key=None):
        self.mq_queue_name = mq_queue_name
        self.product_key = product_key or '*'
        self.mq_initial()
        self.processor = MainProcessor(MainSender(self.product_key))

    def consume(self, ch, method, properties, body):
        log = {
            'ts': time.time(),
            'module': 're_processor_status',
            'running_status': 'beginning'
        }
        try:
            #print body
            gevent.spawn(self.dispatch, body, log)
        except Exception, e:
            console_logger.exception(e)
            log['exception'] = str(e)
            log['proc_t'] = int((time.time() - log['ts']) * 1000)
            logger.info(json.dumps(log))

        #self.channel.basic_ack(delivery_tag=method.delivery_tag)

    def dispatch(self, body, log):
        lst = self.mq_unpack(body, log)
        if lst:
            map(lambda x: gevent.spawn(self.processor.process_msg, x, copy.deepcopy(log)), lst)

    def begin(self):
        self.mq_listen(self.mq_queue_name, self.product_key)


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

    def __init__(self, product_key=None):
        self.mq_queue_name = 'schedule_wait'
        self.product_key = product_key or '*'
        self.mq_initial()
        self.sender = MainSender(self.product_key)

    def consume(self, ch, method, properties, body):
        log = {
            'ts': time.time(),
            'module': 're_processor_status',
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
            logger.info(json.dumps(log))
        finally:
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
            "flag": <timestamp_in_ms int>
        }
        '''
        log['running_status'] = 'waiting'
        msg = json.loads(body)
        print msg

        hour = msg['ts'] / 3600
        minute = msg['ts'] / 60
        file_dir = '{0}/task/{1}/{2}/{3}/{4}'.format(
            settings.SCHEDULE_FILE_DIR.rstrip('/'),
            hour,
            minute,
            msg['ts'],
            hash(msg.get('did') or msg['product_key']) % settings.DEVICE_HASH_GROUP
            )
        if not os.path.exists(file_dir):
            os.makedirs(file_dir, mode=0777)

        file_name = '{0}/{1}_{2}_{3}_{4}_{5}'.format(
            file_dir,
            msg.get('did', ''),
            msg['rule_id'],
            msg['node_id'],
            msg['flag'],
            msg['ts']
            )
        os.system(r'touch {}'.format(file_name))


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
                cache = get_redis()
                lock_key = 're_core_fid_lock'
                lock = cache.setnx(lock_key, 1)
                if lock:
                    with open(fid_name, 'w+') as fp:
                        self.fid = str(uuid.uuid4())
                        fp.write(self.fid)
                    cache.delete(lock_key)
                    break
                else:
                    print 'waiting for fid file...'
                    cache.expire(lock_key, 60)
                    time.sleep(1)

        self.start_time = self.update_start_time()

    def update_start_time(self):
        try:
            return reduce(lambda x, f_lst: min(x, map(lambda y: int(y.split('_')[-1]), f_lst[2])),
                          os.walk('{}/task'.format(settings.SCHEDULE_FILE_DIR.rstrip('/'))),
                          int(time.time()))
        except:
            return int(time.time())

    def begin(self):
        while True:
            try:
                log = {
                    'module': 're_processor_status',
                    'running_status': 'dispatching'
                }
                time_now = time.time()
                ts = int(math.ceil(time_now))
                if time_now > self.start_time:
                    map(lambda x: gevent.spawn(self.scan, x, dict(log, ts=time_now)), xrange(self.start_time, ts+1))
                sleep_remain = ts + 1 - time.time()
                if sleep_remain > 0:
                    time.sleep(sleep_remain)
            except Exception, e:
                console_logger.exception(e)
                log['exception'] = str(e)
                log['proc_t'] = int((time.time() - log['ts']) * 1000)
                logger.info(json.dumps(log))

    def scan(self, ts, log):
        log['running_status'] = 'scan_device'
        hour = ts / 3600
        minute = ts / 60
        dir_name = '{0}/task/{1}/{2}/{3}'.format(
            settings.SCHEDULE_FILE_DIR.rstrip('/'),
            hour,
            minute,
            ts)
        if os.path.isdir(dir_name):
            cache = get_redis()
            lock_key = 're_core_{0}_{1}_lock'.format(self.fid, ts)
            lock = cache.setnx(lock_key, 1)
            if lock:
                msg = {
                    'action_type': 'schedule',
                    'event_type': 'device_schedule',
                    'created_at': time.time()
                }
                msg_lsit = reduce(lambda m_lst, f_lst: m_lst + \
                                  map(lambda x: dict(msg, did=x[0], rule_id=x[1], node_id=x[2]),
                                      map(lambda x: str.spl(x, '_'), f_lst[2])),
                                  os.walk(dir_name),
                                  [])
                if msg_lsit:
                    self.sender.send_msgs(msg_lsit)
                cache.delete(lock_key)
                shutil.rmtree(dir_name)
            else:
                cache.expire(lock_key, 60)


class ProductScheduleScanner(object):
    '''
    dispatch msgs ready to be dispatched
    '''

    def __init__(self, product_key=None):
        self.sender = MainSender(product_key)

    def begin(self):
        log = {
            'module': 're_processor_status',
            'running_status': 'dispatching'
        }
        while True:
            try:
                time_now = time.time()
                min = int(time_now) / 60
                gevent.spawn(self.scan, min, dict(log, ts=time_now))
                sleep_remain = min * 60 + 60 - time.time()
                if sleep_remain > 0:
                    time.sleep(sleep_remain)
            except Exception, e:
                console_logger.exception(e)
                log['exception'] = str(e)
                log['proc_t'] = int((time.time() - log['ts']) * 1000)
                logger.info(json.dumps(log))

    def scan(self, min, log):
        log['running_status'] = 'scan_product'
        db = get_mysql()
        base_sql = 'select `id`, `rule_id_id`, `node` from `{0}` where `next`<={1} order by `id`'.format(
            settings.MYSQL_TABLE['schedule']['table'],
            min)
        update_sql = 'update `{0}` set `next`=`next`+`interval` where `id` in ({1})'
        skip = 0
        limit = 100
        while True:
            sql = ' '.join([
                base_sql,
                ('skip {}'.format(skip) if skip > 0 else ''),
                'limit {}'.format(limit)
            ])
            db.execute(sql)
            result = db.fetchall()
            if not result:
                break

            msg = {
                'action_type': 'schedule',
                'event_type': 'device_schedule',
                'created_at': log['ts'],
                'did': ''
            }
            skip += limit
            msg_lsit = map(lambda res: dict(msg, rule_id=res[1], node_id=res[2]),
                           result)
            if msg_lsit:
                self.sender.send_msgs(msg_lsit)

            sql = update_sql.format(
                settings.MYSQL_TABLE['schedule']['table'],
                ','.join([x[0] for x in result])
                )
            db.execute(sql)

        db.close()
