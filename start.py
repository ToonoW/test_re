#!/usr/bin/env python
# coding=utf-8
"""
Usage:
  start.py [options]

Options:
  -h --help                      Show this screen.
  --version                      Show version.
  --queue=<queue>                binding queue [default: all]
  --routing_key=<routing_key>    routing_key
  --product_key=<product_key>    binding product_key [default: *]
  --only-tmp-consumer            start as a tmp consumer
  --only-http-consumer           start as a http consumer
  --only-gdmshttp-consumer       start as a gdms_http consumer
  --only-devctrl-consumer        start as a devctrl consumer
  --only-es-consumer             start as a es consumer
  --only-device-scanner          start as a device scanner
  --only-product-scanner         start as a product scanner
"""

from gevent import monkey
monkey.patch_all()

import dotenv
dotenv.read_dotenv()

import sys, os
sys.path.append(os.environ.get('SYS_PATH', '.'))

import gevent
from docopt import docopt

from re_processor import settings
from re_processor.consumer import HttpConsumer, TmpConsumer, GDMSHttpConsumer, DevCtrlConsumer, ESConsumer
from re_processor.main import MainDispatcher, DeviceScheduleScanner


if '__main__' == __name__:
    args = docopt(__doc__, version='D3 Processor 0.3.5')
    print args
    product_key = args['--product_key'] if args.has_key('--product_key') else '*'
    routing_key = args['--routing_key'] if args.has_key('--routing_key') else None
    if args['--only-tmp-consumer']:
        TmpConsumer(settings.PUBLISH_ROUTING_KEY['tmp']).start()
    elif args['--only-http-consumer']:
        HttpConsumer(settings.PUBLISH_ROUTING_KEY['http']).start()
    elif args['--only-gdmshttp-consumer']:
        GDMSHttpConsumer(settings.PUBLISH_ROUTING_KEY['gdms_http']).start()
    elif args['--only-devctrl-consumer']:
        DevCtrlConsumer(settings.PUBLISH_ROUTING_KEY['devctrl']).start()
    elif args['--only-es-consumer']:
        ESConsumer(settings.PUBLISH_ROUTING_KEY['es']).start()
    elif args['--only-device-scanner']:
        DeviceScheduleScanner(product_key).begin()
    else:
        mq_queue_name = args['--queue'] if args.has_key('--queue') else 'all'
        if 'all' == mq_queue_name:
            obj = MainDispatcher(mq_queue_name, product_key=product_key)
            obj.init_rules_cache()
            gevent.joinall([
                gevent.spawn(obj.update_product_key_set),
                gevent.spawn(obj.begin)
            ])
        else:
            obj = MainDispatcher(mq_queue_name, product_key=product_key, routing_key=routing_key)
            if routing_key == 'products.*.events.device.*':
                obj.init_rules_cache()
            if settings.IS_USE_GEVENT:
                gevent.joinall([
                    gevent.spawn(obj.begin),
                    gevent.spawn(obj.update_product_key_set)
                ])
            else:
                obj.begin()
                obj.update_product_key_set()
