#!/usr/bin/env python
# coding=utf-8
"""
Usage:
  start.py [options]

Options:
  -h --help                      Show this screen.
  --version                      Show version.
  --queue=<queue>                binding queue [default: all]
  --product_key=<product_key>    binding product_key
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
    queue = args['<queue>'] if args.has_key('<queue>') else 'all'
    product_key = args['<product_key>'] if args.has_key('<product_key>') else None
    gevent.joinall([
        gevent.spawn(get_container(queue, product_key=product_key, container_type='main').begin),
        gevent.spawn(get_container('tri', product_key=product_key, container_type='output').begin),
        gevent.spawn(get_container('sel', product_key=product_key, container_type='internal').begin),
        gevent.spawn(get_container('cal', product_key=product_key, container_type='internal').begin),
        gevent.spawn(get_container('que', product_key=product_key, container_type='internal').begin),
        gevent.spawn(get_container('log', product_key=product_key, container_type='internal').begin)
    ])
