#!/usr/bin/env python
# coding=utf-8

import json, requests, time

import gevent

from re_processor import settings
from re_processor.consumer.BaseConsumer import BaseRabbitmqConsumer
from re_processor.common import update_virtual_device_log


class DevCtrlConsumer(BaseRabbitmqConsumer):

    def multi_ctrl(self, result, value):
        if 200 != result[0]:
            return result

        res = 0, ''
        if 'delay' in value:
            try:
                time.sleep(int(value['delay']))
            except ValueError:
                pass
            res = 200, ''

        if 'did' in value:
            body = {
                'source': 'd3'
            }
            if 'attrs' in value:
                body['attrs'] = value['attrs']
            elif 'raw' in value:
                body['raw'] = value['raw']

            url = ''.join(['http://', settings.HOST_GET_BINDING, '/v1/device/{}/control'.format(value['did'])])
            headers = {
                'Authorization': settings.INNER_API_TOKEN
            }
            resp = requests.post(url, data=json.dumps(body), headers=headers)
            res = resp.status_code, resp.content

        return res

    def process(self, body, log=None):
        #print body
        msg = json.loads(body)

        if 'devctrl' != msg['action_type']:
            log['exception'] = 'Invalid action_type: {}'.format(msg['action_type'])
            return False

        log['action_type'] = msg['action_type']
        params = msg.get('params', {})
        content = msg.get('content', '{}')

        for key, val in params.items():
            content = content.replace('"${'+key+'}"', json.dumps(val))
        log['content'] = content
        try:
            content = json.loads(content)
            status_code, resp_content = reduce(self.multi_ctrl,
                                               content['value'] if content.get('multi', False) is True else [dict(content['value'], did=msg['did'])],
                                               (200, ''))
        except Exception, e:
            if 'log_data' in msg:
                msg['log_data']['exception'] = str(e)
                update_virtual_device_log(**msg['log_data'])
        else:
            log['status_code'] = status_code
            log['resp_content'] = resp_content
            if 'log_data' in msg:
                if 200 != status_code:
                    msg['log_data']['exception'] = resp_content

                update_virtual_device_log(**msg['log_data'])
