#!/usr/bin/env python
# coding=utf-8
"""
Usage:
  start.py [options]

Options:
  -h --help                      Show this screen.
  --version                      Show version.
  --queue=<queue>                binding queue [default: all]
  --product_key=<product_key>    binding product_key [default: *]
  --only-tmp-consumer            start as a tmp consumer
  --only-http-consumer           start as a http consumer
  --only-gdmshttp-consumer       start as a gdms_http consumer
  --only-devctrl-consumer        start as a devctrl consumer
  --only-es-consumer             start as a es consumer
  --main=<main>                  set num of main core
  --sel=<sel>                    set num of sel core
  --cal=<cal>                    set num of cal core
  --script=<script>              set num of script core
  --json=<script>                set num of json core
  --que=<que>                    set num of que core
  --log=<log>                    set num of log core
  --tri=<log>                    set num of log core
"""

from gevent import monkey
monkey.patch_all()

import dotenv
dotenv.read_dotenv()

import sys, os
sys.path.append(os.environ.get('SYS_PATH', '.'))

import gevent
from gevent.queue import Queue
from docopt import docopt

from re_processor import settings
from re_processor.consumer import HttpConsumer, TmpConsumer, GDMSHttpConsumer, DevCtrlConsumer, ESConsumer
from re_processor.container import get_container


if '__main__' == __name__:
    args = docopt(__doc__, version='RulesEngine Processor 0.2.2')
    print args
    if args['--only-tmp-consumer']:
        TmpConsumer(settings.PUBLISH_ROUTING_KEY['tmp']).start()
    elif args['--only-http-consumer']:
        HttpConsumer(settings.PUBLISH_ROUTING_KEY['http']).start()
    elif args['--only-gdmshttp-consumer']:
        greenlet_list = []
        greenlet_list.extend([gevent.spawn(GDMSHttpConsumer(settings.PUBLISH_ROUTING_KEY['gdms_http']).start) for i in range(10)])
        gevent.joinall(greenlet_list)
    elif args['--only-devctrl-consumer']:
        greenlet_list = []
        greenlet_list.extend([gevent.spawn(DevCtrlConsumer(settings.PUBLISH_ROUTING_KEY['devctrl']).start) for i in range(10)])
        gevent.joinall(greenlet_list)
    elif args['--only-es-consumer']:
        greenlet_list = []
        greenlet_list.extend([gevent.spawn(ESConsumer(settings.PUBLISH_ROUTING_KEY['es']).start) for i in range(10)])
        gevent.joinall(greenlet_list)
    else:
        mq_queue_name = args['--queue'] if args.has_key('--queue') else 'all'
        product_key = args['--product_key'] if args.has_key('--product_key') else '*'

        start_unit = settings.START_UNIT
        default_queue = {x: Queue() for x in start_unit}
        start_unit[mq_queue_name] = 'main'

        greenlet_list = []
        for name, c_type in settings.START_UNIT.items():
            if 'main' == c_type and args['--main']:
                greenlet_list.extend([gevent.spawn(get_container(name, default_queue, product_key=product_key, container_type=c_type).begin) for i in range(int(args['--main']))])
            elif 'main' != c_type and args['--{}'.format(name)]:
                greenlet_list.extend([gevent.spawn(get_container(name, default_queue, product_key=product_key, container_type=c_type).begin) for i in range(int(args['--{}'.format(name)]))])
            else:
                greenlet_list.append(gevent.spawn(get_container(name, default_queue, product_key=product_key, container_type=c_type).begin))

        gevent.joinall(greenlet_list)
