#!/usr/bin/env python
# coding=utf-8

from gevent import monkey
monkey.patch_all()

import dotenv
dotenv.read_dotenv()

import sys, os
sys.path.append(os.environ.get('SYS_PATH', '.'))

import gevent

#from re_processor import settings
from re_processor.container import get_container


if '__main__' == __name__:
    queue = 'all'
    gevent.joinall([
        gevent.spawn(get_container(queue, container_type='main').begin),
        gevent.spawn(get_container('tri', container_type='output').begin),
        gevent.spawn(get_container('sel', container_type='internal').begin),
        gevent.spawn(get_container('cal', container_type='internal').begin),
        gevent.spawn(get_container('que', container_type='internal').begin),
        gevent.spawn(get_container('log', container_type='internal').begin)
    ])
