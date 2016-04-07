#!/usr/bin/env python
# coding=utf-8

#import dotenv
#dotenv.read_dotenv()

from getenv import env

# debug
DEBUG = env('DEBUG', False)

# project path
SYS_PATH = env('SYS_PATH', '.')

# M2M settings
M2M_MQ_URL = env('M2M_MQ_URL', 'amqp://guest:guest@m2mprod.gwdev.com:5672/mqtt')

EXCHANGE = env('EXCHANGE', 'amq.topic')

START_UNIT = {
    'sel': 'internal',
    'cal': 'internal',
    'que': 'internal',
    'log': 'internal',
    'tri': 'output'
}

TOPIC_MAP = {
    'device_online': 'online',
    'device_offline': 'offline',
    'device_bind': 'bind',
    'device_unbind': 'unbind',
    'device_status_kv': 'data',
    'attr_fault': 'fault',
    'attr_alert': 'alert'
}

ROUTING_KEY = {
    'all': 'products.{}.events.device.#',
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

PUBLISH_ROUTING_KEY = {
    'notification': 'gw_notification_message',
    'http': 'rules_engine_http'
}

DEBUG_ROUTING_KEY = {
    'notification': 'rules_engine_debug',
    'http': 'rules_engine_debug',
    'log': 'rules_engine_debug'
}

# where msg to send
MSG_TO = {
    #'internal': 'redis',
    'internal': 'default',
    'external': 'rabbitmq'
}

#
TRANSCEIVER = {
    'send': {
        'redis': 'redis_publish',
        'rabbitmq': 'mq_publish',
        'default': 'default_publish'
    },
    'begin': {
        'redis': 'redis_listen',
        'rabbitmq': 'mq_listen',
        'default': 'default_listen'
    },
    'unpack': {
        'redis': 'redis_unpack',
        'rabbitmq': 'mq_unpack',
        'default': 'default_unpack'
    },
    'init': {
        'redis': 'redis_initial',
        'rabbitmq': 'mq_initial',
        'default': 'default_initial'
    },
}

# container map
CONTAINER_MAP = {
    'internal': {
        #'queue': ['BaseRedismqConsumer'],
        'queue': ['DefaultQueueConsumer'],
        'processor': ['CommonProcessor'],
        'transceiver': ['InternalTransceiver']
    },
    'main': {
        #'queue': ['BaseRabbitmqConsumer', 'BaseRedismqConsumer'],
        'queue': ['BaseRabbitmqConsumer', 'DefaultQueueConsumer'],
        'processor': ['CommonProcessor'],
        'transceiver': ['MainTransceiver']
    },
    'output': {
        #'queue': ['BaseRabbitmqConsumer', 'BaseRedismqConsumer'],
        'queue': ['BaseRabbitmqConsumer', 'DefaultQueueConsumer'],
        'processor': ['CommonProcessor'],
        'transceiver': ['OutputTransceiver']
    }
}

# processor core_map
CORE_MAP = {
    'internal': {
        'sel': 'SelectorCore',
        'cal': 'CalculatorCore',
        'que': 'QueryCore',
        'tri': 'TriggerCore',
        'log': 'LoggerCore'
    }
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
MYSQL_TABLE = {
    'rule': {
        'table': 't_rulesengine_rule',
    },
    'action': {
        'table': 't_rulesengine_action'
    }
}

# redis
REDIS_HOST = env("REDIS_HOST", 'localhost')
REDIS_PORT = env("REDIS_PORT", 6379)
REDIS_DB = env("REDIS_DB", 0)
REDIS_PWD = env("REDIS_PWD", '')
REDIS_EXPIRE = env("REDIS_EXPIRE", 3600)
LISTEN_TIMEOUT = env("LISTEN_TIMEOUT", 20)

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
        'file': {
            'level': 'INFO',
            'when': 'D',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': '/mnt/workspace/gw_re_pocessor/processor.log',
            'formatter': 'standard',
        },
        "console": env("LOG_CONSOLE", {"level": "INFO", "class": "logging.StreamHandler", "formatter": "standard"}),
        #"graylog": env("LOG_GRAYLOG", {"level": "INFO", "class": "graypy.GELFRabbitHandler", "url": "amqp://guest:guest@localhost:5672/%2f"}),
    },
    'loggers': {
        'processor': {
            'handlers': ['file'],
            'level': 'INFO',
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
    },
    'tri': {
        'action_type': 1,
        'params': 2,
        'action_content': 3,
        'action_id': 4
    },
    'tri_in_db': {
        'allow_time': 1,
        'task_list': 2,
        'action_type': 3,
        'params': 4,
        'action_content': 5
    },
    'log': {}
}

