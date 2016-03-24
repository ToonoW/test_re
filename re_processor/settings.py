#!/usr/bin/env python
# coding=utf-8

import dotenv
dotenv.read_dotenv()

from getenv import env

# project path
SYS_PATH = env('SYS_PATH', '.')

# M2M settings
M2M_MQ_URL = env('M2M_MQ_URL', 'amqp://guest:guest@m2mprod.gwdev.com:5672/mqtt')

EXCHANGE = env('EXCHANGE', 'amq.topic')

ROUTING_KEY = {
    'enterprises': 'enterprises.{}.events',
    'alert': 'products.{}.events.device.attr_fault',
    'fault': 'products.{}.events.device.attr_alert',
    'online': 'products.{}.events.device.online',
    'offline': 'products.{}.events.device.offline',
    'bind': 'products.{}.events.device.bind',
    'unbind': 'products.{}.events.device.unbind',
    'raw': 'products.{}.events.device.status.raw',
    'data': 'products.{}.events.device.status.kv',
    'changed': 'products.{}.events.datapoints.changed'
}

# databases settings
# mongo
MONGO_DATABASES = env("MONGO_GIZWITS_DATA", "mongodb://localhost:27017/gizwits_data")

# mysql
MYSQL_HOST = env("MYSQL_HOST", "localhost")
MYSQL_PORT = env("MYSQL_PORT", 3306)
MYSQL_USER = env("MYSQL_USER", "root")
MYSQL_PWD = env("MYSQL_PWD", "root")
MYSQL_DB = env("MYSQL_DB", "rules_engine")

# redis
REDIS_HOST = env("REDIS_HOST", 'localhost')
REDIS_PORT = env("REDIS_PORT", 6379)
REDIS_DB = env("REDIS_DB", 0)
REDIS_PWD = env("REDIS_PWD", '')
REDIS_EXPIRE = env("REDIS_EXPIRE", 3600)

# logging
LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {
            'format': "[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s",
            'datefmt': "%d/%b/%Y %H:%M:%S"
        },
    },
    'handlers': {
        "console": env("LOG_CONSOLE", {"level": "INFO", "class": "logging.StreamHandler", "formatter": "standard"}),
        #"graylog": env("LOG_GRAYLOG", {"level": "INFO", "class": "graypy.GELFRabbitHandler", "url": "amqp://guest:guest@localhost:5672/%2f"}),
    },
    'loggers': {
        'file': {
            #'class': 'logging.handlers.TimedRotatingFileHandler',
            'handlers': ['console'],
            'filename': '/mnt/workspace/gw_re_pocessor/processor.log',
            'formatter': 'standard',
        },
        'processor': {
            'handlers': ['console'],
            'level': 'WARN',
        },
        'processor_gray': {
            #'handlers': ['graylog'],
            'handlers': ['console'],
            'level': 'INFO'
        }
    }
}

# task element index
INDEX = {
    'sel': {
        'left': 1,
        'opt': 2,
        'right': 3
    },
    'cal': {
        'exp': 1,
        'name': 2
    },
    'que': {
        'type': 1,
        'target': 2
    }
}