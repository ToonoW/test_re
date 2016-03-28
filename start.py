#!/usr/bin/env python
# coding=utf-8
"""
Usage:
  start.py [options] <queue>

Options:
  -h --help          Show this screen.
  --version          Show version.
  --speed=<kn>
"""

from gevent import monkey
monkey.patch_all()

import dotenv
dotenv.read_dotenv()

import sys, os
sys.path.append(os.environ.get('SYS_PATH', '.'))

import gevent
from docopt import docopt

#from re_processor import settings
from re_processor.container import get_container


if '__main__' == __name__:
    args = docopt(__doc__, version='RulesEngine Processor 0.1.0')
    print args
    queue = 'all'
    gevent.joinall([
        gevent.spawn(get_container(queue, container_type='main').begin),
        gevent.spawn(get_container('tri', container_type='output').begin),
        gevent.spawn(get_container('sel', container_type='internal').begin),
        gevent.spawn(get_container('cal', container_type='internal').begin),
        gevent.spawn(get_container('que', container_type='internal').begin),
        gevent.spawn(get_container('log', container_type='internal').begin)
    ])
