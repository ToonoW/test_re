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


@app.task(ignore_result=True)
def delay_sender(msg, product_key):
    """
    延时任务执行离线消息发送
    """
    sender = MainSender()
    sender.send(msg, product_key)
