#!/usr/bin/env python
# coding=utf-8

from re_processor.mixins import processor, transceiver
from re_processor import settings


class BaseContainer(object):
    '''
    base container for mixins
    '''

    def __init__(self, queue, product_key=None):
        self.queue = queue
        self.product_key = product_key or '*'


# todo
def get_container(queue, product_key=None, container_type='main'):
    container = BaseContainer(queue, product_key)
    bases = [getattr(transceiver, x) for x in settings.CONTAINER_MAP[container_type]['queue']] + /
        [getattr(processor, x) for x in settings.CONTAINER_MAP[container_type]['processor']] + /
        [getattr(transceiver, x) for x in settings.CONTAINER_MAP[container_type]['transceiver']]
    container.__base__ += tuple(bases)
    container.init_queue()
    return container