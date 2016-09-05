#!/usr/bin/env python
# coding=utf-8

import json, requests

from re_processor.consumer.BaseConsumer import BaseRabbitmqConsumer

class HttpConsumer(BaseRabbitmqConsumer):

    def process(self, body, log=None):
        #print body
        msg = json.loads(body)
        if 'http' != msg['action_type']:
            raise Exception('Invalid action_type: {}'.format(msg['action_type']))
        log['product_key'] = msg['product_key']
        log['did'] = msg['did']
        log['action_type'] = msg['action_type']
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
