#!/usr/bin/env python
# coding=utf-8

import json, requests

from re_processor import settings
from re_processor.consumer.BaseConsumer import BaseRabbitmqConsumer
from re_processor.common import update_virtual_device_log


class DevCtrlConsumer(BaseRabbitmqConsumer):

    def process(self, body, log=None):
        #print body
        msg = json.loads(body)

        if 'devctrl' != msg['action_type']:
            raise Exception('Invalid action_type: {}'.format(msg['action_type']))

        log['action_type'] = msg['action_type']
        params = msg.get('params', {})
        content = msg.get('content', '{}')
        url = ''.join(['http://', settings.HOST_GET_BINDING, '/v1/device/{}/control'.format(msg['did'])])
        headers = {
            'Authorization': settings.INNER_API_TOKEN
        }

        for key, val in params.items():
            content = content.replace('"${'+key+'}"', json.dumps(val))
        log['content'] = content
        content = json.loads(content)

        resp = requests.post(url, data=json.dumps(content['value']), headers=headers)

        #print resp.content
        #print resp.status_code

        log['resp_content'] = resp.content
        log['status_code'] = resp.status_code

        if 'log_data' in msg:
            update_virtual_device_log(**msg['log_data'])
