#!/usr/bin/env python
# coding=utf-8

import json, time, requests, redis

from re_processor.connections import get_redis
from re_processor.consumer.BaseConsumer import BaseRabbitmqConsumer


class GDMSHttpConsumer(BaseRabbitmqConsumer):

    def get_token(self, key, content, log={}):
        token = None
        cnt = 0
        lock = False
        cache = get_redis()
        p = cache.pipeline()

        while cnt < 3 and not token:
            try:
                token = cache.get(key)
                if not token:
                    lock = cache.setnx(key + '_lock', 'lock')
                    if lock:
                        token_url = content['token_url'] + '?app_id={}&secret_key={}'.format(content['app_id'], content['secret_key'])
                        resp_token = requests.get(token_url)
                        if 200 == resp_token.status_code:
                            resp_content = json.loads(resp_token.content)
                            token = resp_content['token']
                            expires_at = resp_content['expires_at'].split('.')
                            expires_at = int(time.mktime(time.strptime(expires_at[0],'%Y-%m-%dT%H:%M:%S')))# * 1000 + int(expires_at[1][0:-1])
                            p.set(key, token)
                            p.expire(key, expires_at - int(time.time()) - 5)
                            p.execute()

                        elif 429 == resp_token.status_code:
                            log['status'] = resp_token.status_code
                            log['message'] = resp_token.content
                            cnt += 4
                    else:
                        time.sleep(2)

            except redis.exceptions.RedisError, e:
                log['message'] = 'redis error: {}'.format(str(e))
                raise e
            except requests.exceptions.RequestException, e:
                log['message'] = 'http request error: {}'.format(str(e))
            finally:
                cnt += 1
                if lock:
                    cache.delete(key + '_lock')

        log['retry_cnt_token'] = cnt % 4 - 1

        return token

    def process(self, body, log=None):
        msg = json.loads(body)
        if 'gdms_http' != msg['action_type']:
            log['exception'] = 'Invalid action_type: {}'.format(msg['action_type'])
            return False
        log['product_key'] = msg['product_key']
        log['did'] = msg['did']
        log['action_type'] = msg['action_type']
        content = json.loads(msg['content'])
        log['app_id'] = content['app_id']
        log['secret_key'] = content['secret_key']
        key = 'rules_engine_gdms_apitoken_{}'.format(hash(content['app_id'] + content['secret_key']))
        url = content['url']
        data = content.get('data', {})
        headers = content.get('headers', {})
        timeout = content.get('timeout', 5)
        if content.get('need_params', True):
            data.update({k.split('.')[-1]: v for k, v in msg['params'].items()})

        token = self.get_token(key, content, log)
        if not token:
            log['exception'] = 'Faied to get token: app_id {0}, secret_key {1}'.format(content['app_id'], content['secret_key'])
            return False

        cnt = 0
        headers['Api-Token'] = token
        while cnt < 3:
            try:
                if headers.has_key('Content-Type') and 'application/json' == headers['Content-Type']:
                    resp = requests.post(url, data=json.dumps(data), headers=headers, timeout=timeout)
                else:
                    resp = requests.post(url, data=data, headers=headers, timeout=timeout)

                if 401 == resp.status_code:
                    token = self.get_token(key, content, log)
                    if not token:
                        log['exception'] = 'Faied to get token: app_id {0}, secret_key {1}'.format(content['app_id'], content['secret_key'])
                        return False

                elif 200 == resp.status_code:
                    break
            except requests.exceptions.RequestException, e:
                log['message'] = 'http request error: {}'.format(str(e))
            finally:
                cnt += 1

        log['token'] = token
        log['status'] = resp.status_code
        log['message'] = resp.content
        log['retry_cnt'] = cnt - 1
        #print resp.status_code
        #print resp.content
