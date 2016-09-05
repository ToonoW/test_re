#!/usr/bin/env python
# coding=utf-8

import json, requests

from re_processor import settings
from re_processor.consumer.BaseConsumer import BaseRabbitmqConsumer


class DevCtrlConsumer(BaseRabbitmqConsumer):

    def process(self, body, log=None):
        #print body
        msg = json.loads(body)

        if 'devctrl' != msg['action_type']:
            raise Exception('Invalid action_type: {}'.format(msg['action_type']))
