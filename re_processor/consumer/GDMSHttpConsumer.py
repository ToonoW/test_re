#!/usr/bin/env python
# coding=utf-8

import json, time, requests

from re_processor.connections import get_redis
from re_processor.consumer.BaseConsumer import BaseRabbitmqConsumer


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
            expires_at = resp_content['expires_at'].split('.')
            expires_at = int(time.mktime(time.strptime(expires_at[0],'%Y-%m-%dT%H:%M:%S'))) * 1000 + int(expires_at[1][0:-1])
            redis_conn.zadd(key, token, expires_at)
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
        log['product_key'] = msg['product_key']
        log['did'] = msg['did']
        log['action_type'] = msg['action_type']
        content = json.loads(msg['content'])
        log['app_id'] = content['app_id']
        log['secret_key'] = content['secret_key']
        key = 'rules_engine_gdms_token_{}'.format(hash(content['app_id'] + content['secret_key']))
        cnt = 0
        url = content['url']
        data = content.get('data', {})
        headers = content.get('headers', {})
        timeout = content.get('timeout', 5)
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
                resp = requests.post(url, data=json.dumps(data), headers=headers, timeout=timeout)
            else:
                resp = requests.post(url, data=data, headers=headers, timeout=timeout)

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
