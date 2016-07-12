#!/usr/bin/env python
# coding=utf-8

from re_processor.mixins import processor, transceiver
from re_processor import settings


class BaseContainer(object):
    '''
    base container for mixins
    '''

    def __init__(self, mq_queue_name, default_queue, product_key=None):
        self.mq_queue_name = mq_queue_name
        self.default_queue = default_queue
        self.product_key = product_key or '*'
        self.debug = settings.DEBUG

tmp_class = {}

# todo
def get_container(mq_queue_name, default_queue, product_key=None, container_type='main'):
    if not tmp_class.has_key(container_type):
        bases = [getattr(transceiver, x) for x in settings.CONTAINER_MAP[container_type]['queue']] \
                + [getattr(processor, x) for x in settings.CONTAINER_MAP[container_type]['processor']] \
                + [getattr(transceiver, x) for x in settings.CONTAINER_MAP[container_type]['transceiver']] + [BaseContainer]
        tmp_class[container_type] = type('tmp_class_' + container_type, tuple(bases), {})
    container = tmp_class[container_type](mq_queue_name, default_queue, product_key)
    container.init_queue()
    container.init_processor()
    return container
