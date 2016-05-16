#!/usr/bin/env python
# coding=utf-8

import json, time, requests

from pika import (
    BlockingConnection,
    URLParameters,
    BasicProperties)
from pika.exceptions import AMQPConnectionError

from re_processor import settings
from re_processor.common import debug_logger as logger


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
        self.channel.basic_publish(settings.EXCHANGE, routing_key,
                                   json.dumps(message),
                                   properties=BasicProperties(delivery_mode=2))
        log['proc_t'] = int((time.time() - log['ts']) * 1000)
        logger.info(json.dumps(log))


class HttpConsumer(BaseRabbitmqConsumer):

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
                    log['http_status'] = resp.status_code
                    log['message'] = resp.content
                    print resp.content
