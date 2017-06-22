# coding=utf-8
from __future__ import absolute_import

import dotenv
dotenv.read_dotenv()

from celery import Celery, platforms
from re_processor import settings

platforms.C_FORCE_ROOT = True
app = Celery('re_processor')
app.config_from_object('re_processor:settings')

from re_processor.sender  import MainSender
from re_processor.common import get_device_offline_ts, clean_device_offline_ts
import time


@app.task(ignore_result=True)
def delay_sender(msg, product_key):
    """
    延时任务执行离线消息发送
    """
    sender = MainSender()
    ts = get_device_offline_ts(msg.get('did'))
    if ts:
        ts = float(ts)
        if time.time() - ts >= msg.get('delay_time'):
            sender.send(msg, product_key)
            clean_device_offline_ts(msg.get('did'))
