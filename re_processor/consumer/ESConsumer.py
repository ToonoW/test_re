#!/usr/bin/env python
# coding=utf-8

import json, requests

from re_processor import settings
from re_processor.consumer.BaseConsumer import BaseRabbitmqConsumer


class ESConsumer(BaseRabbitmqConsumer):

    def process(self, body, log=None):
        print body
        msg = json.loads(body)

        if 'es' != msg['action_type']:
            raise Exception('Invalid action_type: {}'.format(msg['action_type']))

        params = msg.get('params', {})
        content = msg.get('content', '{}')
        for key, val in params.items():
            content = content.replace('"${'+key+'}"', json.dumps(val))
        content = json.loads(content)
        
        resp = requests.post(settings.ES_URL, data=json.dumps(content['value']), verify=False)

        print resp.content
        print resp.status_code
        log['resp_content'] = resp.content
        log['status_code'] = resp.status_code
