#!/usr/bin/env python
# coding=utf-8

import json, requests

from re_processor import settings
from re_processor.consumer.BaseConsumer import BaseRabbitmqConsumer


class TmpConsumer(BaseRabbitmqConsumer):

    def process(self, body, log=None):
        #print body
        msg = json.loads(body)
        if 'tmp' != msg['action_type']:
            raise Exception('Invalid action_type: {}'.format(msg['action_type']))
        log['product_key'] = msg['product_key']
        log['did'] = msg['did']
        log['action_type'] = msg['action_type']
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

