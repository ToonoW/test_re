#!/usr/bin/env python
# coding=utf-8

import json, time, requests

from pika import (
    BlockingConnection,
    URLParameters)
from pika.exceptions import AMQPConnectionError

from re_processor import settings
from re_processor.common import debug_logger as logger
from connections import get_redis


class BaseRabbitmqConsumer(object):

    def __init__(self, queue):
        # connect rabbitmq
        self.queue = queue
        try:
            self.m2m_conn = BlockingConnection(
                URLParameters(settings.M2M_MQ_URL))
        except AMQPConnectionError, e:
            logger.exception(e)
            exit(1)
        self.channel = self.m2m_conn.channel()

    def consume(self, ch, method, properties, body):
        log = {'module': 're_processor',
               'ts': time.time(),
               'running_status': 'tmp'}
        try:
            self.process(body, log)
        except Exception, e:
            logger.exception(e)
            log['exception'] = str(e)
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        log['proc_t'] = int((time.time() - log['ts']) * 1000)
        logger.info(json.dumps(log))

    def process(self, body, log=None):
        print body

    def start(self):
        self.channel.queue_declare(queue=self.queue, durable=True)
        self.channel.queue_bind(
            exchange=settings.EXCHANGE,
            queue=self.queue,
            routing_key=self.queue)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.consume, queue=self.queue)
        self.channel.start_consuming()

    def publish(self, routing_key, message):
        log = {'module': 're_processor',
                   'ts': time.time(),
                   'topic': routing_key}
        self.channel.basic_publish(settings.EXCHANGE, routing_key, json.dumps(message))
        log['proc_t'] = int((time.time() - log['ts']) * 1000)
        logger.info(json.dumps(log))


class TmpConsumer(BaseRabbitmqConsumer):

    def process(self, body, log=None):
        #print body
        msg = json.loads(body)
        if 'tmp' != msg['action_type']:
            raise Exception('Invalid action_type: {}'.format(msg['action_type']))
        content = json.loads(msg['content'])
        tpl = content['template']
        for key, value in msg['params'].items():
            tpl = tpl.replace(r'${' + key + '}', u'{}'.format(value))
        headers = {'Authorization': settings.INNER_API_TOKEN}
        url = "{0}{1}{2}{3}".format('http://', settings.HOST_GET_BINDING, '/v1/bindings/', msg['did'])
        resp = requests.get(url, headers=headers)
        alias = json.loads(resp.content)
        user_alias = {key: {x['uid']: x['dev_alias'] for x in val} for key, val in alias.items()}

        url = "{0}{1}{2}".format('http://', settings.HOST_GET_BINDING, '/v1/apps/{}/users')
        users = {}
        for appid in content['app_id']:
            resp = requests.get(url.format(appid) + '?uids={}'.format(','.join(user_alias[appid].keys())), headers=headers)
            users[appid] = json.loads(resp.content)

        if 'phone' == content['name'] or 'sms' == content['name']:
            for appid, user in users.items():
                for u in user:
                    u['dev_alias'] = user_alias[appid][u['uid']] if user_alias.has_key(appid) and user_alias[appid].has_key(u['uid']) else ''
                    data = {
                        'appid': appid,
                        'users': [u],
                        'msg': {
                            'description': tpl.replace('${alias}', u['dev_alias'])
                        }
                    }

                    #print data

                    resp = requests.post(content['url'], data=json.dumps(data), headers=headers, timeout=5)
                    log['status'] = resp.status_code
                    log['message'] = resp.content
                    print resp.content

        elif 'anxinke' == content['name']:
            for appid, user in users.items():
                for u in user:
                    u['dev_alias'] = user_alias[appid][u['uid']] if user_alias.has_key(appid) and user_alias[appid].has_key(u['uid']) else ''
                    data = {
                        'mobile': u['phone'],
                        'content': tpl.replace('${alias}', u['dev_alias'])
                    }
                    resp = requests.post(content['url'], data=json.dumps(data), headers=headers, timeout=5)
                    log['status'] = resp.status_code
                    log['message'] = resp.content
                    print resp.content


class HttpConsumer(BaseRabbitmqConsumer):

    def process(self, body, log=None):
        #print body
        msg = json.loads(body)
        if 'http' != msg['action_type']:
            raise Exception('Invalid action_type: {}'.format(msg['action_type']))
        content = json.loads(msg['content'])
        method = content.get('method', 'get').lower()
        log['re_http_method'] = method
        if method not in ['get', 'post', 'put', 'delete']:
            raise Exception('Invalid http_method: {}'.format(content['method']))
        url = content.get('url')
        log['url'] = url
        if not url:
            raise Exception('url not found')
        data = content.get('data', {})
        log['re_http_data'] = json.dumps(data)
        headers = content.get('headers', {})
        log['re_http_headers'] = json.dumps(headers)
        need_params = content.get('need_params')
        log['need_params'] = need_params
        if need_params:
            data.update(msg['params'])

        resp = getattr(requests, method)(url, data=data, headers=headers, timeout=5)
        log['status'] = resp.status_code
        log['message'] = resp.content
        print resp.content


class GDMSHttpConsumer(BaseRabbitmqConsumer):

    def get_token(self, key, log={}):
        token = None
        try:
            redis_conn = get_redis()
            token_list = redis_conn.zrange(key, -1, -1)
            token = token_list[0] if token_list else token
        except Exception, e:
            log['message'] = 'redis error'
            raise e

        return token

    def new_token(self, key, content, log={}):
        token = None
        try:
            redis_conn = get_redis()
            token_url = content['token_url'] + '?app_id={}&secret_key={}'.format(content['app_id'], content['secret_key'])
            resp_token = requests.get(token_url)
            resp_content = json.loads(resp_token.content)
            token = resp_content['token']
            created_at = resp_content['created_at'].split('.')
            created_at = int(time.mktime(time.strptime(created_at[0],'%Y-%m-%dT%H:%M:%S'))) + int(created_at[1][0:-1])/1000.0
            redis_conn.zadd(key, token, created_at)
        except Exception, e:
            log['status'] = resp_token.status_code
            log['message'] = resp_token.content
            #print resp_token.status_code
            #print resp_token.content
            raise e

        return token

    def delete_token(self, key, token=None):
        try:
            redis_conn = get_redis()
            if token:
                redis_conn.zrem(key, token)
            else:
                redis_conn.zremrangebyrank(key, 0, -2)
            return True
        except:
            return False

    def process(self, body, log=None):
        msg = json.loads(body)
        if 'gdms_http' != msg['action_type']:
            raise Exception('Invalid action_type: {}'.format(msg['action_type']))
        content = json.loads(msg['content'])
        key = 'rules_engine_gdms_token_{}'.format(hash(content['app_id'] + content['secret_key']))
        cnt = 0
        url = content['url']
        data = content.get('data', {})
        headers = content.get('headers', {})
        if content.get('need_params', True):
            data.update({k.split('.')[-1]: v for k, v in msg['params'].items()})
        while cnt < 5:
            token = self.get_token(key, log)
            if token is None:
                token = self.new_token(key, content, log)
                self.delete_token(key)
            headers['Api-Token'] = token
            #headers['X-Gizwits-Rulesengine-Token'] = '1234'
            if headers.has_key('Content-Type') and 'application/json' == headers['Content-Type']:
                resp = requests.post(url, data=json.dumps(data), headers=headers, timeout=5)
            else:
                resp = requests.post(url, data=data, headers=headers, timeout=5)

            if 401 != resp.status_code:
                break

            self.delete_token(key, token)
            cnt += 1

        log['token'] = token
        log['status'] = resp.status_code
        log['message'] = resp.content
        log['retry_cnt'] = cnt
        #print resp.status_code
        #print resp.content
